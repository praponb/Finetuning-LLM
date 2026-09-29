WITH ProductsBySupplier AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END) AS acme_count, MAX(stock_qty) AS max_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.supplier_name,
    P.acme_count,
    P.max_stock_qty
FROM suppliers s
LEFT JOIN ProductsBySupplier P ON s.id = P.supplier_id
WHERE P.acme_count >= 71
ORDER BY P.max_stock_qty DESC
