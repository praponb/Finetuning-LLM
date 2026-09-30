SELECT p.category, AVG(i.quantity) AS avg_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
GROUP BY p.category
HAVING avg_quantity >= 230
