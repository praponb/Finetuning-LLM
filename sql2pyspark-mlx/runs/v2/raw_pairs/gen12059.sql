SELECT YEAR(signup_date) AS signup_year, COUNT(DISTINCT age) AS unique_citys, COUNT(DISTINCT credit_limit) AS unique_countrys
FROM customers
WHERE age BETWEEN 31 AND 50
GROUP BY 1;
