SELECT status FROM payments WHERE order_id IN (5, 7, 15, 25) UNION ALL SELECT status FROM shipments;
