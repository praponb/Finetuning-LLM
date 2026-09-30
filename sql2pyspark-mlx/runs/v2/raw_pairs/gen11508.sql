SELECT supplier_id, stock_qty_range, last_launch_date
FROM (
    SELECT supplier_id, MAX(stock_qty) - MIN(stock_qty) stock_qty_range, MAX(launch_date) last_launch_date
    FROM products
    GROUP BY supplier_id
) s
WHERE stock_qty_range > 62
ORDER BY last_launch_date
