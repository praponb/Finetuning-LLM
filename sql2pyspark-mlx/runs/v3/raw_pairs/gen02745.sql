WITH products_by_supplier AS (
    SELECT supplier_id, MIN(launch_date) AS min_launch_date
    FROM products
    GROUP BY supplier_id
)
SELECT s.country, pbs.min_launch_date
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY pbs.min_launch_date ASC
