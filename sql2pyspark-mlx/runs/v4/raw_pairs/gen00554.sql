SELECT cost FROM shipments INTERSECT SELECT unit_price cost FROM order_items WHERE product_id IS NOT NULL;
