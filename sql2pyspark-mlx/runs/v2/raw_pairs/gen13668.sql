WITH products_by_supplier AS (
    SELECT supplier_id, AVG(stock_qty) AS avg_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT s.city, pbs.avg_stock_qty
FROM suppliers s
LEFT OUTER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.avg_stock_qty > 390
ORDER BY pbs.avg_stock_qty ASC
