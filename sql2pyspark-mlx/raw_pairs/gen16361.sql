SELECT id, customer_id, order_date, RANK() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_date_rank FROM orders WHERE shipping_fee <= 24 ORDER BY order_date DESC;
