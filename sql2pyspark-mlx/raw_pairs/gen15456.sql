SELECT customer_id FROM orders WHERE amount >= 1330 INTERSECT SELECT customer_id FROM orders WHERE status NOT IN ('PENDING', 'CANCELLED');
