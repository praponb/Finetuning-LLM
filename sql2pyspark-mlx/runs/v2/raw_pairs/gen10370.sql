SELECT i.id, i.last_restock, i.product_id, i.quantity FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 11677 AND 15352)
