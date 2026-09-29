WITH SupplierStats AS (
    SELECT supplier_id, COUNT(price) AS price_count, ROUND((100 * SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home
    FROM products
    GROUP BY supplier_id
)
SELECT s.country, S.price_count, S.pct_home
FROM suppliers s
INNER JOIN SupplierStats S ON s.id = S.supplier_id
WHERE S.pct_home > 41
ORDER BY country DESC
