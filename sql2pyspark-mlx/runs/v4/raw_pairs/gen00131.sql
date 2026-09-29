WITH ranked_store_sales AS (SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units) AS rn FROM store_sales) SELECT * FROM ranked_store_sales WHERE rn = 1;
