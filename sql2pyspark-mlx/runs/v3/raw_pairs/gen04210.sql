SELECT p.brand, AVG(i.quantity) AS avg_quantity, COUNT(DISTINCT i.quantity) AS unique_products
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.brand
ORDER BY p.brand;
