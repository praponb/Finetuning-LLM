SELECT s.city, AVG(ss.revenue) AS avg_revenue
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE ss.sale_date >= '2025-01-01'
GROUP BY s.city;
