WITH products_by_supplier AS (
    SELECT supplier_id, ROUND(AVG(cost), 2) AS avg_cost, MAX(launch_date) AS latest_launch_date
    FROM products
    WHERE SIZE(tags) > 1
    GROUP BY supplier_id
)
SELECT s.rating, pbs.avg_cost, pbs.latest_launch_date
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.avg_cost > 73;
