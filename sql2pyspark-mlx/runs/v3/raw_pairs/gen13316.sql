SELECT s.city, MAX(i.last_restock) last_last_restock, MAX(i.quantity) - MIN(i.quantity) quantity_range
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
WHERE p.category = 'Fashion'
GROUP BY s.city
