SELECT MONTH(c.signup_date) AS signup_month, COUNT(DISTINCT s.customer_id) AS unique_customers
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY MONTH(c.signup_date)
ORDER BY unique_customers
