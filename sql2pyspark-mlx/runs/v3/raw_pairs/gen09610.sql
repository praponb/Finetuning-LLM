WITH product_totals AS (
    SELECT product_id, AVG(quantity) AS avg_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    p.supplier_id,
    pt.avg_quantity,
    pt.quantity_range
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.quantity_range >= 38
ORDER BY p.id
