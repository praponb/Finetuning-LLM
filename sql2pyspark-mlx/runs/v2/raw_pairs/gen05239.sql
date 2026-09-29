SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, SUM(revenue) AS total_revenue, DENSE_RANK() OVER (ORDER BY SUM(revenue) DESC) AS rnk
FROM store_sales
GROUP BY DATE_TRUNC('MONTH', sale_date)
ORDER BY rnk
