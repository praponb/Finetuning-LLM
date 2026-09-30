SELECT id, order_id, cost, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS rn FROM shipments;
