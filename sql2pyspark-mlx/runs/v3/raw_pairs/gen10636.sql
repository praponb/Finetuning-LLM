SELECT i.id, i.product_id FROM inventory i WHERE i.quantity <= 154 AND i.store_id NOT IN (SELECT id FROM stores WHERE sqft BETWEEN 9146 AND 12845) ORDER BY i.id DESC, product_id DESC;
