SELECT i.id, i.last_restock, i.store_id, i.product_id FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE MONTH(opened_date) IN (1, 4, 11)) ORDER BY 4 DESC, id LIMIT 20;
