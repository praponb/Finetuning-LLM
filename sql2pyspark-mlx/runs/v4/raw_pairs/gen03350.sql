SELECT status FROM orders WHERE status = 'PENDING' UNION SELECT status FROM shipments ORDER BY status DESC;
