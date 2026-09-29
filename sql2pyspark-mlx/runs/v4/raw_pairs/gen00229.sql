SELECT s.country, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
JOIN products p ON p.id = i.product_id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY stddev_quantity DESC, s.country DESC
LIMIT 1;
