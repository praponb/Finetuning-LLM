WITH supplier_summary AS (
    SELECT supplier_id, SUM(price) AS total_price
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.rating, ss.total_price
FROM suppliers s
JOIN supplier_summary ss ON s.id = ss.supplier_id
ORDER BY total_price DESC, id
LIMIT 3
