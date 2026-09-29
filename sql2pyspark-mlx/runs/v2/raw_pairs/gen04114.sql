SELECT opened_date, sqft, region, CONCAT_WS('/', city, store_name) AS city_store_name_key FROM stores WHERE opened_date BETWEEN '2024-02-01' AND '2024-12-01' ORDER BY sqft DESC;
