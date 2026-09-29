SELECT customer_id
FROM subscriptions
WHERE end_date > '2023-04-01' AND customer_id IN (3, 28)
