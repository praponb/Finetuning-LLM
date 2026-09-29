SELECT id, product_id, revenue, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS row_num FROM store_sales;
