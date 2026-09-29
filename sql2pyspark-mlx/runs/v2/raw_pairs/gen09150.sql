SELECT orders.id, orders.shipping_fee FROM orders WHERE orders.store_id IN (SELECT id FROM stores WHERE sqft = 7975);
