WITH supplier_summary AS (
    SELECT supplier_id, COUNT(*) AS product_count
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.city,
    ss.product_count
FROM suppliers s
JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.product_count > 5
ORDER BY product_count;
