WITH products_by_supplier AS (
    SELECT supplier_id, MAX(stock_qty) AS max_stock_qty
    FROM products
    WHERE supplier_id = 4
    GROUP BY supplier_id
)
SELECT s.city, s.country, pbs.max_stock_qty
FROM suppliers s
JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
