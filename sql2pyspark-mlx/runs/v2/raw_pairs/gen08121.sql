SELECT p.id, p.brand
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND MONTH(i.last_restock) IN (6, 7, 8))
