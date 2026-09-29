SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS seq_no, AVG(quantity) OVER (PARTITION BY store_id) AS store_id_avg_quantity FROM inventory
