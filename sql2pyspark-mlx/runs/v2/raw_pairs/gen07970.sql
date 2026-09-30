SELECT s.country, SUM(CASE WHEN p.brand = 'Umbrella' THEN 1 ELSE 0 END) umbrella_count
FROM products p
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY country, 2 DESC;
