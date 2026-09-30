SELECT order_id, status
FROM shipments
WHERE status IN ('IN_TRANSIT', 'DELIVERED') AND order_id IS NULL;
