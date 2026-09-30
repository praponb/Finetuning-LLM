WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS pos FROM shipments WHERE order_id IS NULL) SELECT * FROM ranked WHERE pos <= 1
