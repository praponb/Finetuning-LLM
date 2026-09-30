SELECT i.id, i.store_id, i.quantity, i.product_id FROM inventory i WHERE i.quantity < (SELECT AVG(i2.quantity) FROM inventory i2 WHERE i2.store_id = i.store_id) ORDER BY i.id;
