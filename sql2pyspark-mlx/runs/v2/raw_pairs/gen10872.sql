WITH products_by_supplier AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
    FROM products
    WHERE stock_qty IS NULL
    GROUP BY supplier_id
)
SELECT s.city, pbs.brand_values
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY brand_values DESC
