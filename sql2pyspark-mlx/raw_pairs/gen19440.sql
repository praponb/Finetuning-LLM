SELECT
    YEAR(signup_date) AS signup_year,
    MAX(credit_limit) AS max_credit_limit,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values,
    COUNT(DISTINCT credit_limit) AS unique_citys
FROM customers
WHERE city IN ('London', 'Singapore', 'New York') AND city IN ('Berlin', 'Singapore', 'Bangkok')
GROUP BY signup_year
ORDER BY signup_year DESC;
