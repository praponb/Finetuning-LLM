SELECT region, SORT_ARRAY(COLLECT_SET(city)) AS city_list, COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count, COUNT(sqft) AS sqft_count FROM stores GROUP BY region;
