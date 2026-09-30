SELECT status, 'shipment' AS record_type
FROM shipments
WHERE shipped_date < '2023-07-01'
UNION
SELECT status, 'subscription' AS record_type
FROM subscriptions;
