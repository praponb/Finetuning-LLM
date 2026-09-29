WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY cost DESC) AS rn FROM shipments) SELECT id, order_id, cost FROM numbered WHERE rn <= 3;
