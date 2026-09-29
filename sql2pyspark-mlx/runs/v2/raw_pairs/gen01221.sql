SELECT country, MAX(rating) max_rating, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) city_values
FROM suppliers
WHERE (country = 'JP' AND supplier_name LIKE 'Gl%') OR supplier_name LIKE '%City%'
GROUP BY 1
HAVING MAX(rating) >= 2.7
ORDER BY city_values DESC;
