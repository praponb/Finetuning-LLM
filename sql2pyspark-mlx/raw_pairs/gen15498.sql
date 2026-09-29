SELECT id, store_id, amount, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount, id) AS row_num FROM orders;
