SELECT store_id, DATE_TRUNC('MONTH', sale_date) AS sale_month, AVG(revenue) AS avg_revenue
FROM store_sales
WHERE product_id IN (4, 6, 10)
GROUP BY store_id, DATE_TRUNC('MONTH', sale_date)
ORDER BY avg_revenue DESC, store_id ASC, sale_month ASC
LIMIT 50;
