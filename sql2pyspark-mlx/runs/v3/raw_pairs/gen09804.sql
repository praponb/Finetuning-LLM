SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, ROUND(AVG(age), 2) AS avg_age
FROM customers
WHERE country = 'US'
GROUP BY 1
HAVING ROUND(AVG(age), 2) >= 61
