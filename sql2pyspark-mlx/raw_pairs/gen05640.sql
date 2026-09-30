SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values, COUNT(DISTINCT credit_limit) unique_segments
FROM customers
GROUP BY city
HAVING COUNT(DISTINCT credit_limit) > 1;
