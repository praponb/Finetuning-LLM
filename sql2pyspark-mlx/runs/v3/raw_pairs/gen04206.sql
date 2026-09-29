SELECT city, CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM suppliers
WHERE supplier_name LIKE 'Sia%' AND supplier_name LIKE '%Import%'
GROUP BY city, CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket, city_values, city
LIMIT 10
