SELECT start_date
FROM subscriptions
WHERE status = 'CANCELLED'
UNION
SELECT opened_date
FROM stores
