WITH supplier_summary AS (
    SELECT supplier_id, AVG(price) avg_price
    FROM products
    GROUP BY supplier_id
)
SELECT s.supplier_name, s.rating, ss.avg_price
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.avg_price >= 480
