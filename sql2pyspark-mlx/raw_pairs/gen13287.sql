SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS rn FROM shipments QUALIFY rn = 1
