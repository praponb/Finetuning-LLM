WITH products_by_supplier AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) AS category_list, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range
    FROM products
    WHERE stock_qty > 167
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    s.city,
    pbs.category_list,
    pbs.stock_qty_range
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id;
