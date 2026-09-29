SELECT status, 'shipment' AS source FROM shipments WHERE carrier = 'Kerry' UNION ALL SELECT status, 'subscription' AS source FROM subscriptions
