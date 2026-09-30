SELECT
    s.id,
    s.supplier_name,
    s.city,
    t.total_cost
FROM suppliers s
INNER JOIN (SELECT supplier_id, SUM(cost) AS total_cost
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id;
