SELECT p.category, COUNT(i.quantity) AS quantity_count, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT OUTER JOIN products p ON p.id = i.product_id
GROUP BY p.category
ORDER BY quantity_range DESC, category
LIMIT 10;
