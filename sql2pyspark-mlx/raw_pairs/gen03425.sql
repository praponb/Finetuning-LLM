SELECT id, store_id, order_date, discount, SUM(discount) OVER (PARTITION BY store_id ORDER BY order_date, id) AS running_discount FROM orders WHERE customer_id = 39;
