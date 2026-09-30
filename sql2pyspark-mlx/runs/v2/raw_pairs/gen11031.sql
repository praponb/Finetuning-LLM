SELECT id, quantity, FLOOR(quantity) AS quantity_whole
FROM order_items
WHERE unit_price BETWEEN 160 AND 210
