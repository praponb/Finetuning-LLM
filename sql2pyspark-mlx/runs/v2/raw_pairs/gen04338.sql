SELECT status, 'shipment' AS source FROM shipments WHERE status IN ('DELIVERED', 'IN_TRANSIT') UNION SELECT status, 'subscription' AS source FROM subscriptions ORDER BY source
