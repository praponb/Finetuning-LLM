SELECT CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, s.country, SUM(p.price) AS total_price
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.category = 'Grocery'
GROUP BY CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END, s.country
