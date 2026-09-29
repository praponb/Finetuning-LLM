WITH products_by_supplier AS (
    SELECT supplier_id, SUM(cost) AS total_cost
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.rating, pbs.total_cost
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.total_cost >= 1500;
