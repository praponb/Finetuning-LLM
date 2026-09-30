SELECT shipping_fee FROM orders INTERSECT SELECT cost AS shipping_fee FROM shipments;
