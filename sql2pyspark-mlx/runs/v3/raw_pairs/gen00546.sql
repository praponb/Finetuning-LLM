SELECT start_date, 'subscription' source
FROM subscriptions
UNION ALL
SELECT delivered_date start_date, 'shipment' source
FROM shipments;
