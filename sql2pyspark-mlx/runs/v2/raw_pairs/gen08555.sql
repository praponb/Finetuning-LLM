SELECT p.brand, COUNT(i.quantity) AS quantity_count, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
GROUP BY p.brand
