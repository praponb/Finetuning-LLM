WITH supplier_summary AS (
    SELECT supplier_id, ROUND((100 * SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sports, COUNT(*) AS num_products
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    s.country,
    ss.pct_sports,
    ss.num_products
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
ORDER BY s.supplier_name DESC
