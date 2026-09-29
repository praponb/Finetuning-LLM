SELECT id, order_id, unit_price, quantity, LAG(quantity, 2, 0) OVER (PARTITION BY order_id ORDER BY unit_price, id) prev_quantity FROM order_items WHERE unit_price IS NOT NULL;
