SELECT id, order_id, product_id, quantity FROM order_items WHERE product_id = 2 AND unit_price <= 300 ORDER BY 1 LIMIT 5;
