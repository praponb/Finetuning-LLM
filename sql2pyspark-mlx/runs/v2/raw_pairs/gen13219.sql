WITH products_by_supplier AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, SUM(stock_qty) AS total_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.rating, products_by_supplier.brand_values, products_by_supplier.total_stock_qty
FROM suppliers
INNER JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id
ORDER BY rating DESC;
