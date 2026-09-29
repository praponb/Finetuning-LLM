SELECT s.rating, t.cost_range
FROM suppliers s
LEFT JOIN (SELECT supplier_id, MAX(cost) - MIN(cost) AS cost_range
    FROM products
    WHERE launch_date BETWEEN '2024-12-25' AND '2025-06-10'
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY rating DESC, t.cost_range
