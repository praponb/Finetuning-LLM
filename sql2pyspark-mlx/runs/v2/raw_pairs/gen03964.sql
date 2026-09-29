WITH order_totals AS (
    SELECT order_id, AVG(quantity) AS avg_quantity, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.store_id,
    ot.avg_quantity,
    ot.avg_unit_price
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id
