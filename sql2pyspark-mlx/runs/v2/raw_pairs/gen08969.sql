SELECT
    supplier_id,
    DATE_TRUNC('MONTH', launch_date) launch_month,
    MIN(stock_qty) min_stock_qty,
    COUNT(DISTINCT cost) unique_suppliers,
    SORT_ARRAY(COLLECT_SET(brand)) brand_list
FROM products
WHERE cost <= 630 AND ARRAY_CONTAINS(tags, 'sale')
GROUP BY 1, 2
HAVING min_stock_qty < 240
