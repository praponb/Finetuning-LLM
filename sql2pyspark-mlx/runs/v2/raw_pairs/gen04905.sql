SELECT YEAR(signup_date) AS signup_year, MAX(credit_limit) AS max_credit_limit, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM customers
GROUP BY YEAR(signup_date)
HAVING MAX(credit_limit) > 6300
ORDER BY 2;
