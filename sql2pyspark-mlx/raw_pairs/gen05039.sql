WITH products_by_supplier AS (
    SELECT supplier_id, SUM(stock_qty) AS total_stock_qty
    FROM products
    WHERE stock_qty = 303
    GROUP BY supplier_id
)
SELECT s.id, s.supplier_name, pbs.total_stock_qty
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.total_stock_qty > 2200;
