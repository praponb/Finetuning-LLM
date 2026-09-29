SELECT start_date, customer_id
FROM subscriptions
WHERE customer_id IS NOT NULL
EXCEPT
SELECT start_date, customer_id
FROM subscriptions
WHERE end_date > '2025-03-01';
