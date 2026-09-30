SELECT
    s.id,
    s.supplier_name,
    t.pct_acme,
    t.first_launch_date
FROM suppliers s
LEFT JOIN (SELECT supplier_id, ROUND((100 * SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_acme, MIN(launch_date) AS first_launch_date
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.pct_acme > 49
ORDER BY id;
