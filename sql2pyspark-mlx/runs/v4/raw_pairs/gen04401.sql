SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, SUM(revenue) AS total_revenue, SUM(SUM(revenue)) OVER (ORDER BY DATE_FORMAT(sale_date, 'yyyy-MM')) AS cumulative_revenue
FROM store_sales
WHERE units > 29
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY sale_month ASC, total_revenue DESC
