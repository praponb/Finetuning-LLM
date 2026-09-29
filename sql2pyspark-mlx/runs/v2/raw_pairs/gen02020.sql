SELECT status FROM orders INTERSECT SELECT status FROM accounts WHERE customer_id IN (9, 38) ORDER BY status;
