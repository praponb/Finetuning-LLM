WITH supplier_summary AS (
    SELECT supplier_id, MAX(cost) AS max_cost
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.rating, ss.max_cost
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.max_cost >= 280
ORDER BY s.rating
