WITH numbered AS (SELECT id, store_id, last_restock, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS pos FROM inventory) SELECT * FROM numbered WHERE pos = 1 ORDER BY 3 DESC
