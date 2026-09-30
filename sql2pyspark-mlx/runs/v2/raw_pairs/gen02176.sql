WITH supplier_totals AS (
    SELECT supplier_id, MIN(price) AS min_price
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.supplier_name, supplier_totals.min_price
FROM suppliers
INNER JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id
WHERE supplier_totals.min_price > 950;
