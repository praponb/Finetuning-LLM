SELECT id, store_id, order_date FROM orders WHERE customer_id = 11 UNION ALL SELECT id, store_id, order_date FROM orders WHERE customer_id = 8;
