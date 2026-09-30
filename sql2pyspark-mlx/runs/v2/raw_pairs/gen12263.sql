SELECT p.category, SUM(ss.revenue) AS total_revenue
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id
WHERE p.category IN ('Fashion', 'Books')
GROUP BY p.category
