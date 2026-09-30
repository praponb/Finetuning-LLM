WITH top_orders AS (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee) AS pos FROM orders) SELECT * FROM top_orders WHERE pos <= 3;
