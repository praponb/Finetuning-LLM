SELECT id, cost, carrier
FROM shipments
WHERE order_id IS NOT NULL;
