SELECT id, store_id, COUNT(*) OVER (PARTITION BY store_id) AS store_id_count FROM store_sales ORDER BY id DESC
