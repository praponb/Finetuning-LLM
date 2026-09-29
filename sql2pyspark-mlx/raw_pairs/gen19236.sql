WITH products_by_supplier AS (
    SELECT supplier_id, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
    FROM products
    WHERE category IN ('Electronics', 'Sports')
    GROUP BY supplier_id
)
SELECT s.rating, pbs.stddev_stock_qty
FROM suppliers s
JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY stddev_stock_qty DESC;
