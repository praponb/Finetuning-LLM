SELECT shipped_date
FROM shipments
WHERE shipped_date IS NOT NULL
EXCEPT
SELECT start_date AS shipped_date
FROM subscriptions
WHERE start_date IS NOT NULL
