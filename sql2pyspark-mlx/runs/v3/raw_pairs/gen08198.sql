WITH supplier_stats AS (
    SELECT supplier_id, COUNT(*) AS cnt, COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count
    FROM products
    WHERE supplier_id IN (13, 21, 30, 33)
    GROUP BY supplier_id
)
SELECT s.country, ss.cnt, ss.acme_count
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.cnt > 9;
