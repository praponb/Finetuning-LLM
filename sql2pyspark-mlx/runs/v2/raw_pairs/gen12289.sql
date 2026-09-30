SELECT i.id, i.store_id, i.quantity, i.last_restock FROM inventory i WHERE i.quantity = (SELECT MIN(i2.quantity) FROM inventory i2 WHERE i2.store_id = i.store_id) ORDER BY i.quantity;
