SELECT s.country, SUM(CASE WHEN p.category = 'Home' THEN 1 ELSE 0 END) AS home_count, MAX(p.price) - MIN(p.price) AS price_range
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY 1 ASC
LIMIT 5
