WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(DISTINCT cost) AS unique_brands, MIN(stock_qty) AS min_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.supplier_name,
    pbs.unique_brands,
    pbs.min_stock_qty
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY rating DESC, unique_brands
