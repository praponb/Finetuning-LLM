SELECT MIN(sale_date) AS min_sale_date, COUNT(*) AS row_count, COUNT(revenue) AS revenue_count
FROM store_sales
WHERE product_id = 1
ORDER BY revenue_count DESC;
