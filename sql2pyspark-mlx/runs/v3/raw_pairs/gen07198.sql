SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) city_values, COUNT(DISTINCT sqft) unique_regions, COUNT(sqft) sqft_count FROM stores GROUP BY city ORDER BY 3, city LIMIT 1;
