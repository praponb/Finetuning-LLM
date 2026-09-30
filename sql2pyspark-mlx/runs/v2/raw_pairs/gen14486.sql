SELECT s.city, MIN(i.last_restock) AS min_last_restock, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN stores s ON i.store_id = s.id
GROUP BY s.city
ORDER BY quantity_range DESC, city
LIMIT 5
