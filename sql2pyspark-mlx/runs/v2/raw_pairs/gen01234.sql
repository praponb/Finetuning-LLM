SELECT MONTH(opened_date) AS opened_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
FROM stores
WHERE UPPER(store_name) LIKE '%IRP%'
GROUP BY opened_month;
