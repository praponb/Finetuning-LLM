SELECT store_id, COUNT(quantity) quantity_count FROM inventory GROUP BY store_id ORDER BY quantity_count DESC, store_id DESC LIMIT 10
