SELECT orders.id, orders.customer_id, orders.discount FROM orders WHERE orders.store_id NOT IN (SELECT id FROM stores WHERE sqft > 13134)
