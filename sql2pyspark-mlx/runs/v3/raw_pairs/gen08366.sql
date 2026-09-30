SELECT store_sales.product_id, MIN(inventory.last_restock) AS min_last_restock
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY store_sales.product_id
ORDER BY min_last_restock ASC;
