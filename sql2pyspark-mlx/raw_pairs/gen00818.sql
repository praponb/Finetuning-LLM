SELECT channel, customer_id FROM orders WHERE store_id IS NULL UNION ALL SELECT channel, customer_id FROM orders WHERE order_date > '2024-08-03' ORDER BY customer_id DESC
