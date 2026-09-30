WITH supplier_summary AS (
    SELECT supplier_id, COUNT(*) AS row_count
    FROM products
    GROUP BY supplier_id
)
SELECT s.city, ss.row_count
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.row_count >= 7
