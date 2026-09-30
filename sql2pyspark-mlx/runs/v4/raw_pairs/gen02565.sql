WITH supplier_stats AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list, MAX(stock_qty) AS max_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.city,
    s.rating,
    ss.brand_list,
    ss.max_stock_qty
FROM suppliers s
LEFT OUTER JOIN supplier_stats ss ON s.id = ss.supplier_id
ORDER BY city DESC
