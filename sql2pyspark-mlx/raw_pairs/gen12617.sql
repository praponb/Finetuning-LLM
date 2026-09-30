SELECT store_id, product_id, MAX(last_restock) AS latest_last_restock FROM inventory GROUP BY store_id, product_id ORDER BY latest_last_restock;
