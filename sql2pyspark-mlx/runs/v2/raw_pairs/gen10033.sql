SELECT status FROM accounts INTERSECT SELECT status FROM shipments WHERE order_id IN (4, 14, 17, 34);
