SELECT i.id, i.last_restock, i.product_id, i.store_id FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE opened_date < '2023-07-01');
