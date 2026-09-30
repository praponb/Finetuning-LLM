WITH product_totals AS (
    SELECT product_id, MIN(quantity) AS min_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    p.stock_qty,
    pt.min_quantity,
    pt.quantity_range
FROM products p
LEFT OUTER JOIN product_totals pt ON p.id = pt.product_id;
