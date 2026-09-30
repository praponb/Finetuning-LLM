WITH supplier_totals AS (
    SELECT supplier_id, MAX(stock_qty) max_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.rating,
    suppliers.city,
    supplier_totals.max_stock_qty
FROM suppliers
INNER JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id
WHERE supplier_totals.max_stock_qty > 170
ORDER BY suppliers.city ASC, id ASC
LIMIT 20
