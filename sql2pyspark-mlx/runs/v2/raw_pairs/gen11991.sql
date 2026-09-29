SELECT id, customer_id, end_date
FROM subscriptions
WHERE start_date < '2024-04-01'
EXCEPT
SELECT id, customer_id, end_date
FROM subscriptions
WHERE MONTH(start_date) IN (2, 5, 9)
