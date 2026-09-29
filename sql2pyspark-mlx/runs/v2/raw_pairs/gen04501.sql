SELECT city, COUNT(*) AS num_stores, ROUND(AVG(sqft), 2) AS avg_sqft, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values FROM stores GROUP BY city ORDER BY city DESC LIMIT 5;
