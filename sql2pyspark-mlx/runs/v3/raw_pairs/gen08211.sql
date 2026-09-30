SELECT s.country, CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(p.cost) AS cost_count
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country, CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END
HAVING COUNT(p.cost) > 2
ORDER BY country DESC, rating_bucket
LIMIT 5;
