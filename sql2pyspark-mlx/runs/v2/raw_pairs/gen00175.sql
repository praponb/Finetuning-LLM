WITH numbered AS (SELECT id, store_id, revenue, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS rn FROM store_sales) SELECT id, store_id, revenue FROM numbered WHERE rn <= 2;
