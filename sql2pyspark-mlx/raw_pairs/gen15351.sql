WITH SupplierSummary AS (
    SELECT supplier_id, MIN(launch_date) AS first_launch_date
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.supplier_name,
    suppliers.country,
    SupplierSummary.first_launch_date
FROM suppliers
JOIN SupplierSummary ON suppliers.id = SupplierSummary.supplier_id
ORDER BY supplier_name;
