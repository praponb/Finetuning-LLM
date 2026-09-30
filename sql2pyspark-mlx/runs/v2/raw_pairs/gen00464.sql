WITH products_by_supplier AS (
    SELECT supplier_id, MAX(cost) - MIN(cost) AS cost_range
    FROM products
    GROUP BY supplier_id
)
SELECT s.city, s.country, pbs.cost_range
FROM suppliers s
LEFT OUTER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
