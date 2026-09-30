SELECT delivered_date
FROM shipments
WHERE delivered_date IS NOT NULL
INTERSECT
SELECT end_date AS delivered_date
FROM subscriptions
WHERE end_date IS NOT NULL
ORDER BY delivered_date DESC;
