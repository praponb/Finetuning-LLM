SELECT p.brand, AVG(ss.revenue) AS avg_revenue
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
GROUP BY p.brand
ORDER BY 2 DESC, p.brand DESC;
