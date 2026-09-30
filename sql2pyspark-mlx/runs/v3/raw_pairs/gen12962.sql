SELECT QUARTER(sale_date) AS sale_quarter, SUM(revenue) AS total_revenue, SUM(SUM(revenue)) OVER (ORDER BY QUARTER(sale_date)) AS cumulative_revenue
FROM store_sales
WHERE MONTH(sale_date) IN (8, 10, 12)
GROUP BY QUARTER(sale_date)
ORDER BY sale_quarter DESC
LIMIT 5;
