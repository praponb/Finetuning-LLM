SELECT product_id, store_id, SUM(quantity) AS total_quantity, MIN(last_restock) AS first_last_restock FROM inventory GROUP BY 1, 2 HAVING SUM(quantity) > 670
