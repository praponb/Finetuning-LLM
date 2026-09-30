SELECT region, COUNT(sqft) AS sqft_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores GROUP BY 1 ORDER BY region_values DESC;
