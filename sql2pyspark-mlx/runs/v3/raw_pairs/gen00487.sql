SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, COUNT(*) AS total_count, COUNT(revenue) AS revenue_count
FROM store_sales
GROUP BY sale_month
ORDER BY total_count;
