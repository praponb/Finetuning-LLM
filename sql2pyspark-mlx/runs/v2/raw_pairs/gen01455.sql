WITH supplier_summary AS (
    SELECT supplier_id, MIN(cost) AS min_cost
    FROM products
    GROUP BY supplier_id
)
SELECT s.supplier_name, s.country, ss.min_cost
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.min_cost >= 580;
