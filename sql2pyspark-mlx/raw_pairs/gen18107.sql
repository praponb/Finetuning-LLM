SELECT status, 'shipment' AS source
FROM shipments
WHERE shipped_date BETWEEN '2023-09-07' AND '2024-04-24'
UNION ALL
SELECT status, 'subscription' AS source
FROM subscriptions
WHERE status = 'PAUSED'
ORDER BY status;
