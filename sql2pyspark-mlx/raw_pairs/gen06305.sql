SELECT CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, s.city, COUNT(ss.revenue) AS revenue_count
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price >= 420
GROUP BY CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END, s.city
HAVING COUNT(ss.revenue) > 4
