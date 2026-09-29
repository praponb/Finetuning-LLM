SELECT country, DATE_TRUNC('MONTH', signup_date) AS signup_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM customers
GROUP BY country, DATE_TRUNC('MONTH', signup_date)
ORDER BY city_values;
