SELECT id, quantity, product_id, ROUND(quantity * 0.9, 2) AS quantity_scaled FROM order_items WHERE product_id IS NULL ORDER BY quantity DESC;
