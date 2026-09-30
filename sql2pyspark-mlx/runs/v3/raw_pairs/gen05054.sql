SELECT
    s.id,
    s.rating,
    s.city,
    t.avg_cost
FROM suppliers s
INNER JOIN (SELECT supplier_id, ROUND(AVG(cost), 2) AS avg_cost
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
