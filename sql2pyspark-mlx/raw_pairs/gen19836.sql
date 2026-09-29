SELECT id, customer_id FROM orders INTERSECT SELECT id, customer_id FROM orders WHERE customer_id IN (9, 17, 24);
