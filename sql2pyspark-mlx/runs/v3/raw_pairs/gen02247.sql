SELECT i.id, i.product_id FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 14572 AND 15984) ORDER BY i.id DESC LIMIT 20;
