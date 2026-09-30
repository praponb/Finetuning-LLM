SELECT start_date
FROM subscriptions
WHERE customer_id IN (18, 25, 35)
EXCEPT
SELECT shipped_date
FROM shipments
WHERE status = 'RETURNED'
ORDER BY start_date DESC
