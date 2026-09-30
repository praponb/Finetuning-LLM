SELECT p.category, MIN(i.quantity) AS min_quantity
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
GROUP BY p.category;
