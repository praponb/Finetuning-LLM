SELECT id, unit_price, ROUND(quantity, 1) AS quantity_rounded FROM order_items WHERE quantity <= 4 ORDER BY unit_price DESC
