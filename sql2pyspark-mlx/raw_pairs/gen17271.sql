SELECT YEAR(signup_date) AS signup_year, SUM(age) AS total_age, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM customers
WHERE email LIKE '%@outlook.com' AND YEAR(signup_date) = 2025
GROUP BY YEAR(signup_date);
