WITH ProductsBySupplier AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.city, ProductsBySupplier.brand_list
FROM suppliers
INNER JOIN ProductsBySupplier ON suppliers.id = ProductsBySupplier.supplier_id
