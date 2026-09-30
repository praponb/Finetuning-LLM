SELECT launch_month, stock_qty_count, total_stock_qty
FROM (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, COUNT(stock_qty) AS stock_qty_count, SUM(stock_qty) AS total_stock_qty
    FROM products
    WHERE cost <= 420
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM')
) AS t
WHERE total_stock_qty > 1300
ORDER BY stock_qty_count, launch_month
LIMIT 5
