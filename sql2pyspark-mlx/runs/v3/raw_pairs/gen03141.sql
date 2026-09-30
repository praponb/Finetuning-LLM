SELECT p.id, p.brand
FROM products p
WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id)
ORDER BY id DESC, brand
LIMIT 3;
