SELECT 'account' AS origin, status FROM accounts UNION ALL SELECT 'shipment' AS origin, status FROM shipments WHERE cost < 31;
