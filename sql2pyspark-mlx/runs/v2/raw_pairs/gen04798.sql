SELECT status, customer_id FROM orders WHERE store_id = 30 UNION SELECT status, customer_id FROM orders WHERE store_id = 19 ORDER BY customer_id DESC;
