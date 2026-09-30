SELECT i.id, i.quantity, i.store_id, i.product_id FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE region IN ('West', 'North', 'East'))
