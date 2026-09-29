SELECT p.id, p.launch_date, p.category
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND YEAR(i.last_restock) = 2023)
ORDER BY p.id DESC, 2
