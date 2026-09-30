WITH product_totals AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    p.stock_qty,
    pt.quantity_range,
    pt.avg_quantity
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id
ORDER BY avg_quantity;
