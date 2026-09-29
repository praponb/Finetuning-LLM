SELECT id, order_id, unit_price, quantity, ROUND(unit_price, 0) AS unit_price_rounded FROM order_items WHERE unit_price BETWEEN 210 AND 340
