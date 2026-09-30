SELECT i.id, i.quantity, i.product_id, i.store_id FROM inventory i WHERE i.store_id NOT IN (SELECT id FROM stores WHERE sqft < 13741);
