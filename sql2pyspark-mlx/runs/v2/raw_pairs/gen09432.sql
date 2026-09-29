SELECT s.city, CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(p.launch_date) AS min_launch_date
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY s.city, CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END
ORDER BY s.city DESC;
