SELECT customer_id, store_id FROM orders WHERE customer_id IS NULL UNION SELECT customer_id, store_id FROM orders WHERE customer_id = 9
