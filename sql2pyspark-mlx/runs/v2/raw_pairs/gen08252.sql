SELECT region, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values, MAX(opened_date) AS last_opened_date FROM stores GROUP BY region ORDER BY last_opened_date DESC;
