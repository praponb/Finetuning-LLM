SELECT id, product_id, unit_price, order_id, COALESCE(quantity, 0) AS quantity_filled FROM order_items ORDER BY unit_price DESC;
