WITH ranked_store_sales AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) pos FROM store_sales) SELECT * FROM ranked_store_sales WHERE pos = 1 ORDER BY 5 DESC;
