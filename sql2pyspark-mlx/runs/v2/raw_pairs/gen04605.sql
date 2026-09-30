WITH SupplierStats AS (
    SELECT supplier_id, COUNT(*) AS cnt
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'sale')
    GROUP BY supplier_id
)
SELECT s.id, s.country, S.cnt
FROM suppliers s
LEFT OUTER JOIN SupplierStats S ON s.id = S.supplier_id
WHERE S.cnt >= 5;
