SELECT MONTH(opened_date) AS opened_month, city, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
WHERE city NOT IN ('Chiang Mai', 'London', 'Singapore') AND (MONTH(opened_date) IN (2, 8, 12) OR city IN ('New York', 'Chiang Mai'))
GROUP BY MONTH(opened_date), city
ORDER BY city ASC, opened_month DESC;
