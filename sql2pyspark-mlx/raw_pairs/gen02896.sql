SELECT id, order_id, unit_price, product_id, COALESCE(unit_price, 0) AS unit_price_filled FROM order_items
