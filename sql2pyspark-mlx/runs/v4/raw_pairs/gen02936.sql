SELECT ss.store_id, MAX(i.last_restock) AS latest_last_restock, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock > '2024-05-01'
GROUP BY ss.store_id
ORDER BY stddev_quantity ASC;
