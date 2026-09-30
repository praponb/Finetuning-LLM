SELECT id, status, store_id, order_date, discount, discount - LAG(discount, 1) OVER (PARTITION BY status, store_id ORDER BY order_date, id) AS discount_change FROM orders;
