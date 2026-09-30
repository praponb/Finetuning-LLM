WITH products_by_supplier AS (
    SELECT supplier_id, ROUND((100 * SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_stark, AVG(cost) AS avg_cost
    FROM products
    WHERE launch_date <= '2024-05-01'
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    pbs.pct_stark,
    pbs.avg_cost
FROM suppliers s
JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY pbs.pct_stark DESC, s.id DESC
