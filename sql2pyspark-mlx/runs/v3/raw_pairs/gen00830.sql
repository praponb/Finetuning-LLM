SELECT p.id, p.price, p.category
FROM products p
WHERE p.brand IS NOT NULL AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id)
ORDER BY category, price DESC, id
LIMIT 20;
