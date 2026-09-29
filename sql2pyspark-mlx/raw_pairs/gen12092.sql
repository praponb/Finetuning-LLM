SELECT id, quantity, ROUND(quantity * 1.07, 2) AS quantity_scaled
FROM order_items
WHERE product_id IN (14, 38);
