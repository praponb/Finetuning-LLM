SELECT status FROM orders EXCEPT SELECT status FROM shipments ORDER BY status;
