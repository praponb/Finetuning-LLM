WITH supplier_stats AS (
    SELECT supplier_id, ROUND((100 * SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grocery, ROUND((100 * SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_stark
    FROM products
    WHERE category IN ('Fashion', 'Sports', 'Home')
    GROUP BY supplier_id
)
SELECT s.supplier_name, ss.pct_grocery, ss.pct_stark
FROM suppliers s
JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.pct_grocery > 58;
