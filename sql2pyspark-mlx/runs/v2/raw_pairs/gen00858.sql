WITH supplier_summary AS (
    SELECT supplier_id, COUNT(DISTINCT stock_qty) AS unique_suppliers
    FROM products
    GROUP BY supplier_id
)
SELECT s.country, ss.unique_suppliers
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
