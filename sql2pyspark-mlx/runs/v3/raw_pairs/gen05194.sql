SELECT p.id, p.brand, p.launch_date FROM products p WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.last_restock >= '2023-05-13') ORDER BY p.launch_date;
