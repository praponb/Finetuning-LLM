SELECT *, ROUND(unit_price * 1.07, 2) unit_price_scaled, COALESCE(unit_price, 0) unit_price_filled FROM order_items WHERE product_id IS NOT NULL AND product_id IN (1, 2, 18, 24) ORDER BY order_id
