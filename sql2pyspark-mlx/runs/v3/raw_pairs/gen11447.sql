SELECT
    s.id,
    s.supplier_name,
    t.min_launch_date,
    t.sports_count
FROM suppliers s
JOIN (SELECT supplier_id, MIN(launch_date) AS min_launch_date, SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END) AS sports_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.sports_count > 27
ORDER BY id DESC, 2 ASC
LIMIT 10;
