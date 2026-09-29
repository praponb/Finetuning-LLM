SELECT status, 'shipment' AS source
FROM shipments
WHERE delivered_date > '2025-10-01'
UNION
SELECT status, 'subscription' AS source
FROM subscriptions
WHERE end_date BETWEEN '2024-10-01' AND '2024-12-01'
