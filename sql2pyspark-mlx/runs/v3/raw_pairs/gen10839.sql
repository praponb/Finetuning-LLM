WITH top_store_sales AS (SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS rnk FROM store_sales) SELECT id, store_id, units, revenue FROM top_store_sales WHERE rnk <= 3;
