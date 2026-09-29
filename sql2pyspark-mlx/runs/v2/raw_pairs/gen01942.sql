SELECT CASE WHEN s.rating >= 3.4 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END rating_bucket, SUM(p.cost) total_cost, COUNT(CASE WHEN p.category = 'Books' THEN 1 END) books_count
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.4 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END
