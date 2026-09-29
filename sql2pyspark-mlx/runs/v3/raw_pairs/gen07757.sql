SELECT status, 'shipment' AS source
FROM shipments
UNION
SELECT status, 'subscription' AS source
FROM subscriptions
WHERE plan <> 'enterprise';
