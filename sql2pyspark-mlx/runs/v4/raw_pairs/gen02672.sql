SELECT store_sales.product_id, MAX(inventory.quantity) AS max_quantity, ROUND(STDDEV(inventory.quantity), 2) AS stddev_quantity
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY store_sales.product_id
HAVING stddev_quantity > 67
ORDER BY product_id, max_quantity DESC
LIMIT 3
