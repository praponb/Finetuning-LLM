SELECT order_date FROM orders WHERE store_id IN (1, 33, 40) UNION ALL SELECT order_date FROM orders WHERE discount IS NULL
