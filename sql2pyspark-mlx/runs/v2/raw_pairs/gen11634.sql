SELECT status, start_date
FROM subscriptions
WHERE status IN ('EXPIRED', 'CANCELLED', 'ACTIVE')
UNION ALL
SELECT status, start_date
FROM subscriptions
WHERE customer_id IN (11, 19);
