SELECT *, CAST(unit_price AS INT) AS unit_price_int, quantity * unit_price AS line_total FROM order_items WHERE order_id = 28 AND quantity >= 6 AND unit_price > 130
