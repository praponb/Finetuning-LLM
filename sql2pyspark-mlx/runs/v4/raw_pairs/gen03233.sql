WITH supplier_summary AS (
    SELECT supplier_id, COUNT(DISTINCT price) AS unique_suppliers
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.supplier_name,
    ss.unique_suppliers
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
