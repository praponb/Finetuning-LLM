WITH supplier_summary AS (
    SELECT supplier_id, MAX(price) - MIN(price) AS price_range, AVG(stock_qty) AS avg_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.city,
    supplier_summary.price_range,
    supplier_summary.avg_stock_qty
FROM suppliers
INNER JOIN supplier_summary ON suppliers.id = supplier_summary.supplier_id
WHERE supplier_summary.avg_stock_qty >= 140;
