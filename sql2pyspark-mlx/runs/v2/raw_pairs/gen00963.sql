WITH products_by_supplier AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END) AS initech_count, COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.country,
    suppliers.supplier_name,
    products_by_supplier.initech_count,
    products_by_supplier.acme_count
FROM suppliers
INNER JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id
WHERE products_by_supplier.initech_count > 70
ORDER BY 4;
