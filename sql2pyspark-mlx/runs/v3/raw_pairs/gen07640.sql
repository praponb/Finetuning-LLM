SELECT id, order_id, cost FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost) AS pos FROM shipments) AS t WHERE pos <= 1
