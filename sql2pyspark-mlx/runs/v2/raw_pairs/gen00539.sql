WITH products_by_supplier AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END) AS umbrella_count
    FROM products
    WHERE stock_qty = 398
    GROUP BY supplier_id
)
SELECT suppliers.id, suppliers.supplier_name, products_by_supplier.umbrella_count
FROM suppliers
INNER JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id
WHERE products_by_supplier.umbrella_count > 79
