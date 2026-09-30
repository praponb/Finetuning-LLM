SELECT store_sales.store_id, MAX(inventory.quantity) - MIN(inventory.quantity) AS quantity_range
FROM inventory
JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.sale_date > '2023-12-03'
GROUP BY store_sales.store_id
ORDER BY quantity_range DESC
