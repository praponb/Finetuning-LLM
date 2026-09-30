SELECT ss.product_id, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id
HAVING quantity_range >= 39
ORDER BY quantity_range DESC;
