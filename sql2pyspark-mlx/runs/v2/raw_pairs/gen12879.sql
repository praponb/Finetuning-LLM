SELECT s.city, CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(ss.units) AS total_units
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.city, CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END
ORDER BY total_units, rating_bucket;
