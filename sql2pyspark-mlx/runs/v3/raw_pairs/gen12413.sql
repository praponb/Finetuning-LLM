SELECT s.supplier_name, t.min_cost, t.num_products
FROM suppliers s
INNER JOIN (SELECT supplier_id, MIN(cost) AS min_cost, COUNT(*) AS num_products
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.num_products >= 4
ORDER BY t.num_products ASC
