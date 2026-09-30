SELECT id, customer_id, status, order_date, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY order_date DESC, id) AS order_date_row FROM orders WHERE customer_id IN (4, 22, 24);
