SELECT s.id, s.browser
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE YEAR(signup_date) >= 2023)
