SELECT MAX(signup_date) AS latest_signup_date, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
FROM customers
WHERE city != 'Bangkok' AND MONTH(signup_date) IN (5, 6, 12) AND city IS NOT NULL
ORDER BY country_values DESC
