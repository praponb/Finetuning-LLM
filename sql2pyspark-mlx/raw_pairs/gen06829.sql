SELECT region, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values, COUNT(sqft) AS sqft_count
FROM stores
GROUP BY region
HAVING COUNT(sqft) >= 1;
