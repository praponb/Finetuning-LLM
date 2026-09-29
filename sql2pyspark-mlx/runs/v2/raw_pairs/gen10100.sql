SELECT store_id, COUNT(*) AS cnt, MIN(last_restock) AS min_last_restock FROM inventory GROUP BY store_id
