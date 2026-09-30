SELECT product_id, COUNT(DISTINCT revenue) AS unique_products FROM store_sales WHERE sale_date < '2023-10-01' OR store_id IN (2, 5) GROUP BY product_id ORDER BY 1 DESC LIMIT 3
