SELECT CASE WHEN s.rating >= 3.5 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, s.city, COUNT(p.price) AS price_count
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.5 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END, s.city
ORDER BY rating_bucket, price_count DESC, city
LIMIT 20;
