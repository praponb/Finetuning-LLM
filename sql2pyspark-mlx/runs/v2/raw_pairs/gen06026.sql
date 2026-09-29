SELECT p.category, AVG(i.quantity) AS avg_quantity, COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
JOIN products p ON p.id = i.product_id
JOIN stores s ON s.id = i.store_id
GROUP BY p.category
HAVING COUNT(DISTINCT i.quantity) > 4
ORDER BY 3 DESC;
