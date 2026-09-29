SELECT id, customer_id, order_date, amount, ROUND(AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_7 FROM orders;
