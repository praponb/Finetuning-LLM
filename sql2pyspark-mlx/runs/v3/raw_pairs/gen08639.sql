SELECT
    s.id,
    s.country,
    t.total_price,
    t.total_cost
FROM suppliers s
LEFT JOIN (SELECT supplier_id, SUM(price) AS total_price, SUM(cost) AS total_cost
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.total_cost > 3500
ORDER BY s.country;
