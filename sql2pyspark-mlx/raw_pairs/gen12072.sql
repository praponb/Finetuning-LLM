WITH order_stats AS (
    SELECT order_id, MIN(shipped_date) AS min_shipped_date, COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END) AS kerry_count
    FROM shipments
    WHERE status NOT IN ('DELIVERED', 'RETURNED')
    GROUP BY order_id
)
SELECT
    o.channel,
    o.amount,
    os.min_shipped_date,
    os.kerry_count
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
WHERE os.kerry_count >= 3
ORDER BY min_shipped_date DESC
