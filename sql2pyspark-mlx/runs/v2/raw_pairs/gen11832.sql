WITH SupplierStats AS (
    SELECT supplier_id, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range, AVG(price) AS avg_price
    FROM products
    WHERE stock_qty = 135
    GROUP BY supplier_id
)
SELECT s.city, S.stock_qty_range, S.avg_price
FROM suppliers s
INNER JOIN SupplierStats S ON s.id = S.supplier_id
ORDER BY stock_qty_range DESC;
