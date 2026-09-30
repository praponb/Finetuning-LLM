SELECT product_id, COUNT(DISTINCT revenue) AS unique_products, AVG(revenue) AS avg_revenue FROM store_sales WHERE MONTH(sale_date) IN (3, 4, 10) GROUP BY product_id ORDER BY product_id ASC LIMIT 10;
