SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values, AVG(credit_limit) AS avg_credit_limit
FROM customers
GROUP BY country
ORDER BY country DESC
LIMIT 10
