WITH products_by_supplier AS (
    SELECT supplier_id, MIN(cost) AS min_cost, AVG(price) AS avg_price
    FROM products
    GROUP BY supplier_id
)
SELECT s.rating, pbs.min_cost, pbs.avg_price
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY pbs.min_cost DESC
