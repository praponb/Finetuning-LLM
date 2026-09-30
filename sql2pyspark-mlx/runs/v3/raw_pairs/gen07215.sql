WITH supplier_summary AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END) grocery_count
    FROM products
    WHERE SIZE(tags) >= 1
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.country,
    suppliers.rating,
    supplier_summary.grocery_count
FROM suppliers
LEFT OUTER JOIN supplier_summary ON suppliers.id = supplier_summary.supplier_id
ORDER BY id DESC;
