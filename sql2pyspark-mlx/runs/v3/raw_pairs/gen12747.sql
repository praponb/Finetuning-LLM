SELECT * FROM (SELECT id, product_id, sale_date, revenue, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS rn FROM store_sales) AS t WHERE rn <= 1
