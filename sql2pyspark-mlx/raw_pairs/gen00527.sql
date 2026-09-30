SELECT
    s.id,
    s.country,
    s.supplier_name,
    t.num_products,
    t.total_cost
FROM suppliers s
INNER JOIN (SELECT supplier_id, COUNT(*) AS num_products, SUM(cost) AS total_cost
    FROM products
    WHERE UPPER(product_name) LIKE '%APT%'
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.total_cost > 3900
ORDER BY country ASC;
