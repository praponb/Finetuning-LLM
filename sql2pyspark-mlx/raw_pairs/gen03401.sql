SELECT product_id, store_id, SUM(revenue) AS total_revenue
FROM store_sales
WHERE sale_date >= '2024-12-01'
GROUP BY 1, 2
ORDER BY total_revenue DESC;
