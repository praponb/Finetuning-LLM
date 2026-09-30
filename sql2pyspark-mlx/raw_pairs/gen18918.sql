SELECT *, CONCAT_WS('/', store_name, region) AS store_name_region_key, CONCAT(region, ' - ', store_name) AS region_store_name
FROM stores
