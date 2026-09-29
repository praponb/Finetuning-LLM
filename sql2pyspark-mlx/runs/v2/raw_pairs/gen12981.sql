SELECT s.country, SUM(CASE WHEN p.category = 'Electronics' THEN 1 ELSE 0 END) AS electronics_count
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price BETWEEN 340 AND 490
GROUP BY s.country
ORDER BY electronics_count
