WITH order_totals AS (
    SELECT order_id, COUNT(*) AS cnt
    FROM shipments
    WHERE carrier = 'Flash'
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    o.shipping_fee,
    ot.cnt
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.cnt > 5;
