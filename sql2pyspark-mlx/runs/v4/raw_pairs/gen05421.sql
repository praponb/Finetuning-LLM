WITH supplier_stats AS (
    SELECT supplier_id, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range, MIN(launch_date) AS min_launch_date
    FROM products
    WHERE stock_qty BETWEEN 131 AND 334
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.city,
    ss.stock_qty_range,
    ss.min_launch_date
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id
ORDER BY id DESC;
