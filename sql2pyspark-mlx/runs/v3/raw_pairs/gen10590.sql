SELECT id, status, order_date, RANK() OVER (PARTITION BY status ORDER BY order_date) AS order_date_rank FROM orders WHERE discount BETWEEN 0.26 AND 0.33;
