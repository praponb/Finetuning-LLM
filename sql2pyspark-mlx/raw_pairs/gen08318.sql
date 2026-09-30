SELECT 'order' AS kind, status FROM orders WHERE shipping_fee <= 15 UNION ALL SELECT 'payment' AS kind, status FROM payments ORDER BY kind
