SELECT id, order_id, unit_price, COALESCE(quantity, 0) AS quantity_filled FROM order_items WHERE unit_price < 370 ORDER BY unit_price DESC;
