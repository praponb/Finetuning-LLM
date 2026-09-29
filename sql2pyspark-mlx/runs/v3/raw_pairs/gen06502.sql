WITH order_totals AS (
    SELECT order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price, MIN(unit_price) AS min_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.discount,
    o.customer_id,
    ot.stddev_unit_price,
    ot.min_unit_price
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.stddev_unit_price > 75
