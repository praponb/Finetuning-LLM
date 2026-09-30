SELECT CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END rating_bucket, COUNT(*) cnt
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE ss.revenue IS NOT NULL
GROUP BY CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket, 2 DESC
