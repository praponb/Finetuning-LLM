SELECT s.city, t.pct_fashion
FROM suppliers s
LEFT JOIN (SELECT supplier_id, ROUND((100 * SUM(CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fashion
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.pct_fashion >= 50
ORDER BY t.pct_fashion DESC, s.city;
