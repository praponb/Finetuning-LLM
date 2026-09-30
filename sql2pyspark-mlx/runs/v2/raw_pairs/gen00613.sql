SELECT city, MONTH(signup_date) AS signup_month, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM customers
WHERE is_active AND age BETWEEN 53 AND 56 AND age = 53
GROUP BY city, MONTH(signup_date)
HAVING ROUND(AVG(credit_limit), 2) >= 69
ORDER BY city, 2 DESC
LIMIT 50
