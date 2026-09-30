SELECT YEAR(signup_date) AS signup_year, COUNT(age) AS age_count FROM customers GROUP BY YEAR(signup_date) HAVING COUNT(age) < 2 ORDER BY age_count DESC, signup_year LIMIT 5;
