SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS row_num FROM inventory) AS r WHERE row_num <= 2 ORDER BY store_id DESC;
