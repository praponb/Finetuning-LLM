SELECT country, SORT_ARRAY(COLLECT_SET(country)) AS country_list, AVG(rating) AS avg_rating
FROM suppliers
WHERE NOT (city = 'Singapore' AND UPPER(supplier_name) LIKE '%URO%')
GROUP BY country
HAVING AVG(rating) > 4.0
ORDER BY avg_rating DESC
