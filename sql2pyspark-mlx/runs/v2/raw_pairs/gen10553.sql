SELECT s.country, COUNT(*) AS inventory_count, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city IN ('London', 'Tokyo')
GROUP BY s.country
ORDER BY country;
