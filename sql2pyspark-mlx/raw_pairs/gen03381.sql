SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS row_num FROM store_sales) AS t WHERE row_num <= 3 ORDER BY revenue DESC;
