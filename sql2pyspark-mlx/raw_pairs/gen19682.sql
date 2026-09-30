SELECT
    s.id,
    s.rating,
    s.supplier_name,
    t.min_cost,
    t.total_cost
FROM suppliers s
LEFT JOIN (SELECT supplier_id, MIN(cost) AS min_cost, SUM(cost) AS total_cost
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.total_cost > 4200
ORDER BY s.rating DESC
