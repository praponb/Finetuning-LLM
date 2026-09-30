WITH SupplierStats AS (
    SELECT supplier_id, COUNT(*) cnt
    FROM products
    WHERE brand IN ('Initech', 'Umbrella')
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.country,
    suppliers.supplier_name,
    SupplierStats.cnt
FROM suppliers
LEFT JOIN SupplierStats ON suppliers.id = SupplierStats.supplier_id;
