SELECT id, order_id, unit_price, ROUND(unit_price * 0.85, 2) AS unit_price_scaled FROM order_items WHERE order_id IN (1, 2, 9, 16) AND product_id IS NULL ORDER BY order_id DESC
