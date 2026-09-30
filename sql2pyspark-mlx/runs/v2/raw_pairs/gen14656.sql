SELECT id, quantity, product_id FROM order_items WHERE NOT (order_id IN (3, 20, 27, 28) AND order_id IN (6, 26) AND unit_price <= 390);
