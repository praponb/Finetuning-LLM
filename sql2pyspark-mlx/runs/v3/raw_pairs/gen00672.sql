SELECT city, SORT_ARRAY(COLLECT_SET(country)) AS country_list, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
FROM suppliers
WHERE supplier_name IS NOT NULL AND rating <= 3.6
GROUP BY 1
HAVING bangkok_count < 74
ORDER BY city;
