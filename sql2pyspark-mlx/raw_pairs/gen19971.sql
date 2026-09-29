SELECT delivered_date, 'shipment' AS kind
FROM shipments
UNION
SELECT start_date, 'subscription' AS kind
FROM subscriptions
ORDER BY delivered_date;
