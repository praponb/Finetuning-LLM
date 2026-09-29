WITH order_totals AS (
    SELECT order_id, ROUND((100 * SUM(CASE WHEN status = 'LOST' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lost
    FROM shipments
    WHERE status = 'LOST'
    GROUP BY order_id
)
SELECT o.status, o.channel, ot.pct_lost
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.pct_lost > 75
ORDER BY o.status;
