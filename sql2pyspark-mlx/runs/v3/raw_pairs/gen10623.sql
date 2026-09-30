SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, MAX(credit_limit) AS max_credit_limit
FROM customers
WHERE signup_date BETWEEN '2024-08-01' AND '2025-05-29'
GROUP BY DATE_TRUNC('MONTH', signup_date)
HAVING MAX(credit_limit) > 15000;
