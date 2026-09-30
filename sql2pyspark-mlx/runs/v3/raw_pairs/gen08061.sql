SELECT status FROM shipments WHERE order_id IS NOT NULL INTERSECT SELECT status FROM subscriptions;
