SELECT 'shipment' AS record_type, status FROM shipments UNION SELECT 'subscription' AS record_type, status FROM subscriptions WHERE plan IN ('basic', 'premium', 'enterprise')
