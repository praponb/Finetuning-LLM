SELECT country, YEAR(signup_date) AS signup_year, COUNT(age) AS age_count FROM customers WHERE LENGTH(name) > 5 GROUP BY country, signup_year ORDER BY age_count, 1;
