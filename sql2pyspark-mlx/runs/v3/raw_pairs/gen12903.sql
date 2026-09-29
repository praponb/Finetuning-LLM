SELECT p.id, p.launch_date, p.product_name
FROM products p
WHERE p.category = 'Books' AND EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.last_restock BETWEEN '2023-07-01' AND '2024-04-11');
