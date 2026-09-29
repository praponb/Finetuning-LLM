SELECT id, store_id, revenue, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS rn FROM store_sales;
