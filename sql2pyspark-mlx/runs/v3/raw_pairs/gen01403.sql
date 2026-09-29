SELECT
    s.id,
    s.country,
    s.city,
    t.avg_cost
FROM suppliers s
LEFT JOIN (SELECT supplier_id, AVG(cost) AS avg_cost
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.avg_cost > 200
ORDER BY avg_cost DESC
