WITH top_orders AS (SELECT id, channel, shipping_fee, DENSE_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) AS pos FROM orders) SELECT * FROM top_orders WHERE pos <= 1
