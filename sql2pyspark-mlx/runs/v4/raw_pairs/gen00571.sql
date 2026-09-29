SELECT id, order_id, FLOOR(quantity) quantity_whole FROM order_items WHERE unit_price IS NOT NULL ORDER BY id DESC
