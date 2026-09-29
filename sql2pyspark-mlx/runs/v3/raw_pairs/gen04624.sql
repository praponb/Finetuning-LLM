WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(*) AS total_count
    FROM products
    WHERE supplier_id IN (4, 15, 36, 38)
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    s.rating,
    pbs.total_count
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.total_count >= 6;
