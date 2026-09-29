SELECT id, unit_price FROM order_items WHERE order_id IN (11, 33) AND quantity BETWEEN 3 AND 7 AND quantity = 6 ORDER BY id DESC LIMIT 20
