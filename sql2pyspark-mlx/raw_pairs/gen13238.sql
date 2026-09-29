SELECT p.category, COUNT(i.quantity) AS quantity_count, COUNT(*) AS total_count
FROM inventory i
JOIN stores s ON i.store_id = s.id
LEFT JOIN products p ON i.product_id = p.id
GROUP BY p.category;
