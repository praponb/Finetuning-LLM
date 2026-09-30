SELECT store_sales.store_id, COUNT(*) AS num_inventory
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY store_sales.store_id;
