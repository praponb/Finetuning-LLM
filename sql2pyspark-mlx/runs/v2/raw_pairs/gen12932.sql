SELECT SORT_ARRAY(COLLECT_SET(city)) AS city_list, COUNT(DISTINCT sqft) AS unique_citys FROM stores WHERE YEAR(opened_date) = 2023 ORDER BY unique_citys;
