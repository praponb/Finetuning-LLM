SELECT CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(DISTINCT p.stock_qty) AS unique_brands, MIN(p.launch_date) AS first_launch_date
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket DESC
LIMIT 5;
