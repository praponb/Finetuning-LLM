SELECT shipping_fee FROM orders WHERE customer_id = 37 UNION SELECT cost FROM shipments WHERE cost > 29;
