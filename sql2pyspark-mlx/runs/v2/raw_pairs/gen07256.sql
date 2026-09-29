WITH supplier_totals AS (
    SELECT supplier_id, MIN(launch_date) min_launch_date, AVG(stock_qty) avg_stock_qty
    FROM products
    WHERE cost BETWEEN 600 AND 700
    GROUP BY supplier_id
)
SELECT
    suppliers.city,
    suppliers.country,
    supplier_totals.min_launch_date,
    supplier_totals.avg_stock_qty
FROM suppliers
LEFT JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id
ORDER BY supplier_totals.avg_stock_qty DESC, suppliers.city;
