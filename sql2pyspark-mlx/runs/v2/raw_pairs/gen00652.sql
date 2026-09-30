WITH ranked AS (SELECT id, product_id, sale_date, revenue, RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS row_num FROM store_sales) SELECT * FROM ranked WHERE row_num <= 5
