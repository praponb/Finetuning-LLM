SELECT i.id, i.quantity FROM inventory i WHERE i.last_restock >= '2024-04-16' AND i.product_id IN (SELECT id FROM products WHERE LENGTH(product_name) < 10) ORDER BY i.id LIMIT 50;
