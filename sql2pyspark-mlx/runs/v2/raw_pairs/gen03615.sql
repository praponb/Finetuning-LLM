SELECT id, end_date, customer_id
FROM subscriptions
WHERE customer_id IS NOT NULL
INTERSECT
SELECT id, end_date, customer_id
FROM subscriptions
WHERE status IN ('CANCELLED', 'EXPIRED', 'PAUSED')
ORDER BY id DESC
