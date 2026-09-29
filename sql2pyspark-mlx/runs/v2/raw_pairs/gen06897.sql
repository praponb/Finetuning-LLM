SELECT s.id, s.country, t.cost_count
FROM suppliers s
JOIN (SELECT supplier_id, COUNT(cost) AS cost_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.cost_count > 3
ORDER BY s.country
