SELECT status, 'shipment' AS source FROM shipments UNION ALL SELECT status, 'subscription' AS source FROM subscriptions ORDER BY status;
