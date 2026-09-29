SELECT s.city, MAX(i.quantity) AS max_quantity, SUM(i.quantity) AS total_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating > 2.8
GROUP BY s.city
