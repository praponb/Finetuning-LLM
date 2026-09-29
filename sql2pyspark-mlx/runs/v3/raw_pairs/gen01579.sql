SELECT p.category, COUNT(*) AS num_store_sales, MAX(ss.revenue) AS max_revenue
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN stores s ON s.id = ss.store_id
WHERE YEAR(s.opened_date) >= 2023
GROUP BY p.category
ORDER BY category DESC
