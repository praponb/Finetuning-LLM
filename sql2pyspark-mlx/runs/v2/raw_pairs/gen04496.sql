SELECT city, region_list, avg_sqft FROM (SELECT city, SORT_ARRAY(COLLECT_SET(region)) AS region_list, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores GROUP BY city) AS x WHERE avg_sqft < 58
