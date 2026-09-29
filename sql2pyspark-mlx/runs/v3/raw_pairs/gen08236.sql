WITH supplier_stats AS (
    SELECT supplier_id, MAX(cost) AS max_cost
    FROM products
    WHERE cost < 500
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    s.country,
    ss.max_cost
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.max_cost > 720;
