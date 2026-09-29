SELECT
    s.city,
    s.supplier_name,
    t.sports_count,
    t.grocery_count
FROM suppliers s
JOIN (SELECT supplier_id, SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END) AS sports_count, COUNT(CASE WHEN category = 'Grocery' THEN 1 END) AS grocery_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.grocery_count > 8
ORDER BY sports_count;
