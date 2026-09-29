WITH products_by_supplier AS (
    SELECT supplier_id, ROUND((100 * SUM(CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_globex
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.supplier_name, pbs.pct_globex
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY s.id DESC;
