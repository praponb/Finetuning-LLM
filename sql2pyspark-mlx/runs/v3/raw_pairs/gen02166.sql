SELECT MIN(opened_date) AS first_opened_date, COUNT(DISTINCT balance) AS unique_customers
FROM accounts
WHERE customer_id IS NULL OR status = 'CLOSED'
ORDER BY first_opened_date, unique_customers
