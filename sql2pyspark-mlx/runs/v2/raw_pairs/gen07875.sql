SELECT CASE WHEN s.rating >= 4.3 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, s.country, COUNT(DISTINCT p.price) AS unique_brands
FROM products p
LEFT JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.rating >= 4.3 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END, s.country
HAVING COUNT(DISTINCT p.price) > 3;
