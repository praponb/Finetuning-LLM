SELECT id, status, order_date, discount, ROUND(AVG(discount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) discount_moving_avg_4 FROM orders;
