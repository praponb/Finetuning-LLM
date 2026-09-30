SELECT id, order_id, shipped_date, carrier FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS pos FROM shipments) AS r WHERE pos <= 2
