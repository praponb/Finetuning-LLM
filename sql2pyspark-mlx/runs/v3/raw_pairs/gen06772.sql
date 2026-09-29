WITH top_store_sales AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS rnk FROM store_sales) SELECT id, product_id, revenue, units FROM top_store_sales WHERE rnk <= 2;
