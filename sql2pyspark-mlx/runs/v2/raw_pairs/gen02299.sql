SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS row_num FROM inventory WHERE MONTH(last_restock) IN (1, 2, 12)) AS r WHERE row_num <= 5;
