SELECT YEAR(sale_date) AS sale_year, COUNT(*) AS store_sale_count FROM store_sales WHERE revenue < 1200 GROUP BY 1 ORDER BY sale_year, store_sale_count DESC LIMIT 50
