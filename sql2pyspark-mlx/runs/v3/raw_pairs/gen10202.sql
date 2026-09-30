SELECT s.city, COUNT(*) AS num_products
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.supplier_id = 36
GROUP BY s.city
