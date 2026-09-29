SELECT status FROM shipments WHERE order_id IS NOT NULL EXCEPT SELECT status FROM subscriptions ORDER BY status DESC;
