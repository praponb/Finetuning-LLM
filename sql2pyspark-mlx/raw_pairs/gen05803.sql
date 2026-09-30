SELECT id, customer_id, order_date, amount, amount - LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS amount_change FROM orders;
