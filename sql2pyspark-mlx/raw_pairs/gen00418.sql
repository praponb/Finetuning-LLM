SELECT
    s.country,
    s.rating,
    t.home_count,
    t.min_cost
FROM suppliers s
LEFT JOIN (SELECT supplier_id, COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count, MIN(cost) AS min_cost
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY min_cost DESC
