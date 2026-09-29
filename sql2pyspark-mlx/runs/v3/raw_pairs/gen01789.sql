SELECT YEAR(signup_date) AS signup_year, SUM(age) AS total_age, ROUND(SUM(age) * 100.0 / SUM(SUM(age)) OVER (), 2) AS pct_of_total FROM customers GROUP BY YEAR(signup_date);
