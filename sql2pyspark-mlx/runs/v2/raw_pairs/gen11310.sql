SELECT CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(*) AS row_count
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE ss.product_id IN (3, 5, 10)
GROUP BY CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END
HAVING COUNT(*) >= 1;
