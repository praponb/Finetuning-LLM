WITH order_totals AS (
    SELECT order_id, ROUND((100 * SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_kerry
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    o.discount,
    ot.pct_kerry
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.pct_kerry > 78
ORDER BY o.id DESC, discount
LIMIT 3
