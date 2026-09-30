WITH products_by_supplier AS (
    SELECT supplier_id, MIN(cost) AS min_cost
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.rating, pbs.min_cost
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.min_cost > 300;
