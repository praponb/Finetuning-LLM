WITH SupplierTotals AS (
    SELECT supplier_id, MIN(launch_date) first_launch_date
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'eco')
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.rating,
    S.first_launch_date
FROM suppliers s
JOIN SupplierTotals S ON s.id = S.supplier_id
