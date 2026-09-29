SELECT product_id, YEAR(sale_date) AS sale_year, MAX(revenue) AS max_revenue, COUNT(units) AS units_count FROM store_sales GROUP BY product_id, YEAR(sale_date)
