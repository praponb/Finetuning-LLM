WITH order_summary AS (
    SELECT order_id, ROUND(AVG(unit_price), 2) AS avg_unit_price, MIN(quantity) AS min_quantity
    FROM order_items
    WHERE quantity >= 8
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    o.order_date,
    os.avg_unit_price,
    os.min_quantity
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id;
