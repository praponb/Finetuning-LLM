SELECT i.id, i.store_id, i.product_id, i.quantity FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE region = 'East') ORDER BY store_id DESC, quantity DESC;
