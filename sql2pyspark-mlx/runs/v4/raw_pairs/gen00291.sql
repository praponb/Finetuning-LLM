SELECT store_sales.store_id, AVG(inventory.quantity) avg_quantity
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY store_sales.store_id
ORDER BY store_sales.store_id DESC, avg_quantity DESC;
