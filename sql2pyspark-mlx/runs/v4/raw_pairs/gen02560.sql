SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, COUNT(DISTINCT quantity) AS unique_products
FROM inventory
WHERE quantity > 111 AND quantity BETWEEN 123 AND 228
GROUP BY store_id
