SELECT s.id, COUNT(p.id) AS product_count, ROUND((100 * SUM(CASE WHEN p.category = 'Home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home
FROM suppliers s
INNER JOIN products p ON s.id = p.supplier_id
GROUP BY s.id
ORDER BY product_count DESC;
