SELECT id, sqft, CONCAT_WS('-', store_name, region) AS store_name_region_key
FROM stores
WHERE NOT (opened_date >= '2025-09-01' AND region IN ('North', 'South'))
ORDER BY store_name_region_key;
