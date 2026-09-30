WITH TopStoreSales AS (SELECT id, store_id, units, product_id, RANK() OVER (PARTITION BY store_id ORDER BY units) AS rn FROM store_sales) SELECT * FROM TopStoreSales WHERE rn = 1;
