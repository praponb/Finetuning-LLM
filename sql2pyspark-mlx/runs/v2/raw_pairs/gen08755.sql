WITH ranked_orders AS (SELECT id, customer_id, amount, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS row_num FROM orders) SELECT * FROM ranked_orders WHERE row_num <= 3;
