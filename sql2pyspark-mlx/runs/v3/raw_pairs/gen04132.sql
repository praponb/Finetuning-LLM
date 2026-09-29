SELECT p.category, AVG(i.quantity) AS avg_quantity
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
GROUP BY p.category
ORDER BY avg_quantity DESC;
