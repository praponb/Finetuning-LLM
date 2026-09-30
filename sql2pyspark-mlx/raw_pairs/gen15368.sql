WITH product_totals AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, MAX(last_restock) AS last_last_restock
    FROM inventory
    WHERE store_id IN (1, 2, 3, 5)
    GROUP BY product_id
)
SELECT
    p.id,
    p.stock_qty,
    p.product_name,
    pt.quantity_range,
    pt.last_last_restock
FROM products p
JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.quantity_range > 44
ORDER BY p.stock_qty
