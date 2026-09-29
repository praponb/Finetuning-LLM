SELECT CASE WHEN s.rating >= 4.3 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, s.city, MIN(p.launch_date) AS first_launch_date
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.3 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END, s.city
ORDER BY rating_bucket ASC, city ASC
LIMIT 20;
