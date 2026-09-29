SELECT id, product_id, order_id, ROUND(unit_price / quantity, 2) AS line_total FROM order_items WHERE quantity > 8
