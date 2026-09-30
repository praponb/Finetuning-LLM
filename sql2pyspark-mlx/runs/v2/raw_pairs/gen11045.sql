WITH supplier_totals AS (
    SELECT supplier_id, COUNT(DISTINCT stock_qty) AS unique_brands, AVG(cost) AS avg_cost
    FROM products
    WHERE price <= 430
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.city,
    supplier_totals.unique_brands,
    supplier_totals.avg_cost
FROM suppliers
LEFT OUTER JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id
ORDER BY supplier_totals.avg_cost DESC
