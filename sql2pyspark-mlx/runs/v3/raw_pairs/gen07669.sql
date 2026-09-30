SELECT region, SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM stores
WHERE (opened_date >= '2024-08-01' AND sqft BETWEEN 6730 AND 14149) OR LENGTH(store_name) >= 5
GROUP BY 1;
