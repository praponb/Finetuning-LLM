SELECT YEAR(sale_date) AS sale_year, COUNT(DISTINCT units) AS unique_products FROM store_sales GROUP BY YEAR(sale_date) ORDER BY sale_year
