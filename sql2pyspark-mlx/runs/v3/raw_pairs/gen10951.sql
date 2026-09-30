WITH supplier_summary AS (
    SELECT supplier_id, MAX(price) max_price
    FROM products
    GROUP BY supplier_id
)
SELECT suppliers.id, suppliers.rating, supplier_summary.max_price
FROM suppliers
JOIN supplier_summary ON suppliers.id = supplier_summary.supplier_id
ORDER BY suppliers.id DESC, suppliers.rating
LIMIT 50
