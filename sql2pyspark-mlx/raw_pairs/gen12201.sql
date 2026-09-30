SELECT cost FROM shipments WHERE status = 'IN_TRANSIT' INTERSECT SELECT shipping_fee FROM orders ORDER BY cost;
