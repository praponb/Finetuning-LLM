SELECT id, product_id, store_id, quantity, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity, id) AS row_num FROM inventory
