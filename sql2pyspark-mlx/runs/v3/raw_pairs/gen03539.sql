SELECT customer_id, end_date
FROM subscriptions
WHERE status <> 'CANCELLED'
UNION
SELECT customer_id, end_date
FROM subscriptions
WHERE start_date >= '2023-12-18'
