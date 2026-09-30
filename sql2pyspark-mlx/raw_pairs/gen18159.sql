SELECT s.id, s.rating, t.cost_range
FROM suppliers s
LEFT JOIN (SELECT supplier_id, MAX(cost) - MIN(cost) AS cost_range
    FROM products
    WHERE brand IN ('Globex', 'Stark')
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.cost_range > 76
ORDER BY t.cost_range DESC
