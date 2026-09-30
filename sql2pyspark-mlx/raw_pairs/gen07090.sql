SELECT id, order_id, delivered_date FROM (SELECT id, order_id, delivered_date, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY delivered_date DESC) AS rn FROM shipments) AS ranked WHERE rn = 1
