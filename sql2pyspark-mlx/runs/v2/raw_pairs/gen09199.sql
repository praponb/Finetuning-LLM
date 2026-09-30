SELECT is_active, MONTH(signup_date) AS signup_month, AVG(credit_limit) AS avg_credit_limit
FROM customers
WHERE MONTH(signup_date) IN (2, 3, 7)
GROUP BY is_active, MONTH(signup_date)
ORDER BY signup_month DESC, is_active
LIMIT 50
