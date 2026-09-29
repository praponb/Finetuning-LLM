WITH supplier_stats AS (
    SELECT supplier_id, COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count
    FROM products
    GROUP BY supplier_id
)
SELECT s.city, ss.fashion_count
FROM suppliers s
JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.fashion_count >= 4;
