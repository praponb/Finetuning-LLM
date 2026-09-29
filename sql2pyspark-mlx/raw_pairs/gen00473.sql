SELECT product_id, MIN(last_restock) AS first_last_restock FROM inventory GROUP BY product_id ORDER BY product_id
