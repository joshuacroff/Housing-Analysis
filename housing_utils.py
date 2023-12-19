import os
import math
import requests
import geopandas as gpd


def pull_geotable_agol(base_url, client, reproject_to_analysis_crs=True):
    """Given the url of an ArcGIS Feature Service layer, pulls the full dataset from ArcGIS Online and return a geopandas GeoDataFrame.

    This function can be used to to pull small to large datasets from ArcGIS Online. In most cases there is a maximum query limit
    set for ArcGIS Feature Service layers which range between 1000 records and 2000 records. This function helps bypass this limit
    by first querying for the full list of object ids, then 'chunking' that list into chunks less than the maximum query limit.

    base_url should be the url of the arcgis feature service layer. For example, the feature service url will look like this:
    https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/DRAFT_TOC_Transit_Stations_Existing_June_2023_/FeatureServer

    Feature services can contain multiple layers. To specify a specific layer, append the layer number to the url. For example, the feature
    service layer url will look like this: https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/DRAFT_TOC_Transit_Stations_Existing_June_2023_/FeatureServer/0

    For more information on feature service layers, see the ArcGIS REST API documentation:
    https://developers.arcgis.com/rest/services-reference/enterprise/layer-feature-service-.htm

    Author: Joshua Croff

    Args:
        base_url: arcgis REST Service url
        client: arcgis client object
        reproject_to_analysis_crs: boolean, if True, will reproject to analysis crs EPSG:26910. Set to true by default. If false, will return data in EPSG:4326.

    Returns:
       geopandas GeoDataFrame: A GeoDataFrame object is a pandas.DataFrame that has a column with geometry.
    """

    # get token from client
    token = client._con.token

    # get feature ids from arcgis api
    id_query = f"query?outFields=*&where=1%3D1&f=json&returnIdsOnly=true&token={token}"
    id_url = os.path.join(base_url, id_query)
    id_response = requests.get(id_url)
    id_json = id_response.json()

    # create list of object ids & chunk to 1000 with is the arcgis api maximum record limit
    id_list = id_json["objectIds"]
    list_len = len(id_list)
    out_list_len = 225
    chunks = math.ceil(list_len / out_list_len)
    print(f"Breaking feature service layer IDs into {chunks} chunks")

    id_chunk_list = [id_list[i : i + out_list_len] for i in range(0, len(id_list), out_list_len)]

    # loop over list of lists and pull data
    # bad_urls = [] # uncomment to track and debug bad urls
    results = []
    for list_item in id_chunk_list:
        obj_str = ",".join(str(x) for x in list_item)
        query = f"query?outFields=*&where=&objectIds={obj_str}&f=geojson&token={token}"
        url = os.path.join(base_url, query)
        response = requests.get(url)
        response.raise_for_status()
        json = response.json()
        results += json["features"]
    # return [results, bad_urls] # uncomment to track and debug bad urls

    gdf = gpd.GeoDataFrame.from_features(results).set_crs("EPSG:4326")

    if reproject_to_analysis_crs:
        gdf = gdf.to_crs("EPSG:26910")

    return gdf


if __name__ == "__main__":
    housing_utils()
