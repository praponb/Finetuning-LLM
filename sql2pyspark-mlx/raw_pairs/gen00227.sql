SELECT id, order_id, product_id, quantity, ROUND(quantity, 0) AS quantity_rounded FROM order_items WHERE order_id IN (3, 18, 36) AND quantity < 3
