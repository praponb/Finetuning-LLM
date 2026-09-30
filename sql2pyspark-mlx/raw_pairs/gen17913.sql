SELECT cost FROM shipments WHERE cost IS NOT NULL UNION SELECT unit_price FROM order_items WHERE unit_price IS NOT NULL ORDER BY cost DESC;
