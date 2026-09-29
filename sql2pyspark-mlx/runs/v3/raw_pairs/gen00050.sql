SELECT delivered_date
FROM shipments
WHERE YEAR(delivered_date) >= 2024
UNION ALL
SELECT start_date
FROM subscriptions
WHERE customer_id IN (5, 17, 37)
ORDER BY delivered_date;
