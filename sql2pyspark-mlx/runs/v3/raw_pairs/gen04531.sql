WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(DISTINCT stock_qty) AS unique_brands, COUNT(DISTINCT price) AS unique_suppliers
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.rating,
    s.supplier_name,
    pbs.unique_brands,
    pbs.unique_suppliers
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.unique_brands >= 6
