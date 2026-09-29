SELECT monthly_fee, 'subscription' AS record_type FROM subscriptions UNION ALL SELECT cost, 'shipment' AS record_type FROM shipments WHERE status IN ('LOST', 'DELIVERED') ORDER BY record_type;
