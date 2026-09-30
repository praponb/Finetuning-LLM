WITH top_store_sales AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS rn FROM store_sales) SELECT * FROM top_store_sales WHERE rn = 1 ORDER BY units DESC, id DESC
