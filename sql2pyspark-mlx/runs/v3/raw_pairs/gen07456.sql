SELECT s.country, MAX(i.last_restock) latest_last_restock, MAX(i.quantity) - MIN(i.quantity) quantity_range
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating <= 2.8
GROUP BY s.country
ORDER BY quantity_range DESC, country
LIMIT 20
