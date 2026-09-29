SELECT s.city, AVG(i.quantity) avg_quantity
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
LEFT JOIN stores s ON i.store_id = s.id
GROUP BY s.city
ORDER BY 2, s.city
