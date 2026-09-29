SELECT o.id, o.shipping_fee FROM orders o WHERE o.discount <= 0.11 AND o.store_id NOT IN (SELECT id FROM stores WHERE sqft < 5978);
