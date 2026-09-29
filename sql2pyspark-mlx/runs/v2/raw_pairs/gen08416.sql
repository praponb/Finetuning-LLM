WITH product_totals AS (
    SELECT product_id, MIN(unit_price) AS min_unit_price, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.stock_qty,
    p.cost,
    pt.min_unit_price,
    pt.stddev_quantity
FROM products p
LEFT OUTER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.min_unit_price >= 190
ORDER BY p.stock_qty
