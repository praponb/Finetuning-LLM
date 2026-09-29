WITH products_by_supplier AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END) AS initech_count
    FROM products
    WHERE launch_date <= '2024-12-01'
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.supplier_name,
    pbs.initech_count
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY supplier_name DESC;
