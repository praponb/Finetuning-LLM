WITH supplier_summary AS (
    SELECT supplier_id, AVG(stock_qty) AS avg_stock_qty, COUNT(*) AS product_count
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.supplier_name,
    ss.avg_stock_qty,
    ss.product_count
FROM suppliers s
INNER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.avg_stock_qty > 220
ORDER BY avg_stock_qty DESC, ss.product_count DESC, id
LIMIT 10
