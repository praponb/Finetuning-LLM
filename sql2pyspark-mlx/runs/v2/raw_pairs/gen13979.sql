WITH product_totals AS (
    SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price, MIN(quantity) AS min_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.price,
    p.stock_qty,
    pt.stddev_unit_price,
    pt.min_quantity
FROM products p
LEFT OUTER JOIN product_totals pt ON p.id = pt.product_id;
