WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS rn FROM shipments) SELECT id, order_id, delivered_date FROM numbered WHERE rn <= 3
