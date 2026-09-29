SELECT s.id, s.supplier_name, t.electronics_count
FROM suppliers s
JOIN (SELECT supplier_id, COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.electronics_count >= 10
ORDER BY electronics_count, id
LIMIT 1
