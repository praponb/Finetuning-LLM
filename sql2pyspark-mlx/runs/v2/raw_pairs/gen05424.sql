SELECT YEAR(sale_date) AS sale_year, COUNT(*) AS num_store_sales
FROM store_sales
WHERE revenue < 2400
GROUP BY YEAR(sale_date)
ORDER BY sale_year ASC, num_store_sales DESC;
