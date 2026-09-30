SELECT YEAR(signup_date) AS signup_year, SUM(age) AS total_age, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(age) DESC) AS rnk FROM customers GROUP BY YEAR(signup_date);
