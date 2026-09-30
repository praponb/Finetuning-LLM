SELECT s.country, ROUND((100 * SUM(CASE WHEN p.brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_acme, SUM(CASE WHEN p.category = 'Home' THEN p.cost ELSE 0 END) AS home_cost
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY s.country;
