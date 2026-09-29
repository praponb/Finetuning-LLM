SELECT MIN(opened_date) AS min_opened_date, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
FROM stores;
