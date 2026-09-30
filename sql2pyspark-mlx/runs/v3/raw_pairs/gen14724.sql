WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity) pos FROM inventory) SELECT id, store_id, quantity FROM numbered WHERE pos <= 3 ORDER BY store_id ASC;
