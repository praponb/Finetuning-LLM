WITH products_by_supplier AS (
    SELECT supplier_id, MIN(launch_date) AS first_launch_date, COUNT(DISTINCT price) AS unique_suppliers
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.city, products_by_supplier.first_launch_date, products_by_supplier.unique_suppliers
FROM suppliers
JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id
WHERE products_by_supplier.unique_suppliers >= 5
ORDER BY first_launch_date ASC
