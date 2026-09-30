SELECT id, order_id, quantity, product_id, unit_price - quantity AS line_total, ROUND(quantity, 0) AS quantity_rounded FROM order_items ORDER BY 6
