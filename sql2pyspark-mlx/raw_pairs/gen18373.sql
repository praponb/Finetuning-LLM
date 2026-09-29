SELECT CASE WHEN s.city IN ('Bangkok', 'Berlin') THEN 'tier_1' ELSE 'other' END AS city_group, ROUND((100 * SUM(CASE WHEN p.brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_acme
FROM products p
RIGHT JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.city IN ('Bangkok', 'Berlin') THEN 'tier_1' ELSE 'other' END
