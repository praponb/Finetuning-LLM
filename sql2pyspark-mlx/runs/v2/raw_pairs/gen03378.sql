SELECT id, quantity, unit_price FROM order_items WHERE unit_price >= 370 AND quantity != 4 ORDER BY 3 ASC, id ASC LIMIT 1
