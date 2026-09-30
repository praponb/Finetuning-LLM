SELECT s.id, s.city, t.pct_sports
FROM suppliers s
LEFT JOIN (SELECT supplier_id, ROUND((100 * SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sports
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.pct_sports >= 77
ORDER BY s.id DESC
