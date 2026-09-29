SELECT status, 'shipment' AS source
FROM shipments
WHERE delivered_date >= '2024-06-01'
UNION
SELECT status, 'subscription' AS source
FROM subscriptions
ORDER BY source
