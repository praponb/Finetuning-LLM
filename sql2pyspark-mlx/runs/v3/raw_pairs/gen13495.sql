SELECT i.id, i.product_id FROM inventory i WHERE i.product_id IN (1, 3, 7) AND i.product_id NOT IN (SELECT id FROM products WHERE launch_date > '2023-03-03') ORDER BY i.id
