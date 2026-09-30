SELECT store_id, MAX(quantity) AS max_quantity, MAX(last_restock) AS latest_last_restock FROM inventory GROUP BY store_id
