SELECT i.id, i.product_id FROM inventory i WHERE i.quantity > 122 AND i.store_id IN (SELECT id FROM stores WHERE store_name LIKE '%Town%');
