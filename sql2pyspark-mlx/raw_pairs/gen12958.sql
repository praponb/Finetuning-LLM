SELECT order_id, product_id, unit_price - quantity line_total FROM order_items WHERE unit_price >= 350 AND product_id IS NULL
