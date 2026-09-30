SELECT customer_id FROM orders WHERE amount <= 810 UNION ALL SELECT customer_id FROM orders WHERE status IN ('SHIPPED', 'PENDING');
