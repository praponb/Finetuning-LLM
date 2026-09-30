WITH products_by_supplier AS (
    SELECT supplier_id, MIN(stock_qty) AS min_stock_qty, COUNT(price) AS price_count
    FROM products
    WHERE cost BETWEEN 250 AND 540
    GROUP BY supplier_id
)
SELECT
    s.supplier_name,
    s.country,
    pbs.min_stock_qty,
    pbs.price_count
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id;
