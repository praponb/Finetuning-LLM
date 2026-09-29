SELECT
    s.id,
    s.city,
    s.rating,
    t.stddev_cost,
    t.umbrella_count
FROM suppliers s
INNER JOIN (SELECT supplier_id, ROUND(STDDEV(cost), 2) AS stddev_cost, SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END) AS umbrella_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.stddev_cost > 77
ORDER BY s.city;
