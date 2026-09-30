WITH top_inventory AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) pos FROM inventory WHERE store_id IN (2, 3, 4, 6)) SELECT * FROM top_inventory WHERE pos <= 3
