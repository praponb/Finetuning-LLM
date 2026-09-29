SELECT customer_id FROM orders WHERE channel != 'store' UNION ALL SELECT customer_id FROM orders WHERE customer_id IS NULL ORDER BY customer_id
