WITH supplier_summary AS (
    SELECT supplier_id, COUNT(*) AS total_count
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.city,
    ss.total_count
FROM suppliers s
JOIN supplier_summary ss ON s.id = ss.supplier_id
ORDER BY s.city;
