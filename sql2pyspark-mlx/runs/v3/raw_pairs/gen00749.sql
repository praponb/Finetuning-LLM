WITH ranked_orders AS (SELECT id, customer_id, order_date, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rnk FROM orders) SELECT * FROM ranked_orders WHERE rnk <= 5
