SELECT product_id, MIN(quantity) AS min_quantity, MAX(last_restock) AS latest_last_restock FROM inventory WHERE product_id = 10 GROUP BY product_id HAVING AVG(quantity) < 210
