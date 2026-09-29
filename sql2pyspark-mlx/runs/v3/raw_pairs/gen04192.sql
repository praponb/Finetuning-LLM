WITH order_summary AS (
    SELECT order_id, MAX(delivered_date) AS last_delivered_date, SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END) AS kerry_count
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    o.discount,
    os.last_delivered_date,
    os.kerry_count
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id;
