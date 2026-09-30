SELECT p.category, MAX(i.last_restock) last_last_restock, ROUND(STDDEV(i.quantity), 2) stddev_quantity
FROM inventory i
RIGHT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
WHERE p.product_name LIKE '%Mat%'
GROUP BY p.category
ORDER BY category DESC;
