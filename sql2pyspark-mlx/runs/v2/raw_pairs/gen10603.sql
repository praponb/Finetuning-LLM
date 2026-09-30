SELECT store_id, SUM(quantity) AS total_quantity, MIN(last_restock) AS first_last_restock FROM inventory WHERE NOT (quantity >= 69 OR quantity >= 119) GROUP BY store_id
