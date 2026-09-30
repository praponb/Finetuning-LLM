SELECT status FROM orders EXCEPT SELECT status FROM shipments WHERE status = 'IN_TRANSIT';
