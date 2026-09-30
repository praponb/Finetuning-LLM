SELECT id, product_id, revenue, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS rn FROM store_sales;
