SELECT status FROM accounts WHERE customer_id IN (8, 18, 23) INTERSECT SELECT status FROM shipments ORDER BY status;
