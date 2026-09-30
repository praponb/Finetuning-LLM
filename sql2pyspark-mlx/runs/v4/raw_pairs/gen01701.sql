SELECT sqft, city, CONCAT(region, ' - ', store_name) AS region_store_name
FROM stores
ORDER BY 1 DESC;
