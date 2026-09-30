SELECT id, unit_price, order_id FROM order_items WHERE unit_price <= (SELECT MIN(unit_price) FROM order_items)
