SELECT p.category, p.brand, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON p.id = i.product_id
WHERE i.product_id IN (1, 3, 7, 8)
GROUP BY p.category, p.brand
HAVING MAX(i.quantity) - MIN(i.quantity) > 39;
