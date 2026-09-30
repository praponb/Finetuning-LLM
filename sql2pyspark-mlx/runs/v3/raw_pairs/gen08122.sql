SELECT status FROM payments WHERE order_id IN (3, 9) UNION ALL SELECT status FROM payments WHERE order_id IS NOT NULL ORDER BY status DESC;
