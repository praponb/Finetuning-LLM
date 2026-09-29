SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, MAX(revenue) AS max_revenue, MAX(sale_date) AS last_sale_date
FROM store_sales
GROUP BY DATE_TRUNC('MONTH', sale_date)
ORDER BY last_sale_date, sale_month
LIMIT 10
