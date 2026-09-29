SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY quantity NULLS FIRST) AS row_num FROM inventory) t WHERE row_num = 1;
