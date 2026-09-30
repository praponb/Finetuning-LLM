WITH supplier_stats AS (
    SELECT supplier_id, MIN(stock_qty) AS min_stock_qty, MAX(stock_qty) AS max_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    ss.min_stock_qty,
    ss.max_stock_qty
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.max_stock_qty > 230
ORDER BY max_stock_qty, id
LIMIT 20
