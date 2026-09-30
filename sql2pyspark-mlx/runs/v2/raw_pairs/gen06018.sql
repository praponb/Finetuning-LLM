WITH products_by_supplier AS (
    SELECT supplier_id, SUM(stock_qty) AS total_stock_qty, ROUND(AVG(price), 2) AS avg_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    pbs.total_stock_qty,
    pbs.avg_price
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id;
