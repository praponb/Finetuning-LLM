SELECT CASE WHEN s.rating >= 3.8 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, s.country, COUNT(DISTINCT p.price) AS unique_brands
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand = 'Umbrella'
GROUP BY CASE WHEN s.rating >= 3.8 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END, s.country
ORDER BY s.country;
