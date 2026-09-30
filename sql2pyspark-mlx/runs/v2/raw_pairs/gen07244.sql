SELECT id, product_id, sale_date, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY sale_date) dense_rnk FROM store_sales WHERE revenue < 3400
