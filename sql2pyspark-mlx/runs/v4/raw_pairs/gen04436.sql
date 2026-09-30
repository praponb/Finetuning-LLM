WITH OrderStats AS (
    SELECT order_id, SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END) AS kerry_count
    FROM shipments
    WHERE delivered_date BETWEEN '2023-10-01' AND '2025-04-01'
    GROUP BY order_id
)
SELECT o.order_date, o.channel, O.kerry_count
FROM orders o
INNER JOIN OrderStats O ON o.id = O.order_id
WHERE O.kerry_count >= 43
ORDER BY channel DESC, order_date
