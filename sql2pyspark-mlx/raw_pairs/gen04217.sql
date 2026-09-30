SELECT CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(ss.units) AS total_units, COUNT(ss.revenue) AS revenue_count
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price >= 420
GROUP BY CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END;
