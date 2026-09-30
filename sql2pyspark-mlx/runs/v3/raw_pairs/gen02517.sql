SELECT end_date, 'subscription' AS source FROM subscriptions UNION SELECT shipped_date, 'shipment' AS source FROM shipments ORDER BY source
