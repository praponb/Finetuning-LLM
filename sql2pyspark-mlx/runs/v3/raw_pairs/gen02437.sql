SELECT CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(p.price) AS max_price, MIN(p.price) AS min_price
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END
