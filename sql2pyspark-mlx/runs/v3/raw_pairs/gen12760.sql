SELECT region, COUNT(sqft) AS sqft_count, COUNT(DISTINCT sqft) AS unique_citys
FROM stores
WHERE store_name LIKE '%Express%' AND LENGTH(store_name) >= 14
GROUP BY 1
