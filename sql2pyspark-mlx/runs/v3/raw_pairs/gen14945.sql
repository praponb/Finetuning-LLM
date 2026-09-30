WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) AS pos FROM orders) SELECT id, customer_id, shipping_fee, status FROM ranked WHERE pos <= 5;
