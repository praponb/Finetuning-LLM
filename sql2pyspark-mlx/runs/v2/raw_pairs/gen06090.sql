SELECT id, quantity FROM order_items WHERE unit_price >= 220 UNION SELECT id, quantity FROM order_items WHERE unit_price <= 380 ORDER BY quantity;
