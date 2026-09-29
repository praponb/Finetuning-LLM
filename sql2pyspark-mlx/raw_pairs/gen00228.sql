WITH ShipmentsByOrder AS (
    SELECT order_id, MIN(delivered_date) min_delivered_date, COUNT(cost) cost_count
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    S.min_delivered_date,
    S.cost_count
FROM orders o
INNER JOIN ShipmentsByOrder S ON o.id = S.order_id
WHERE S.cost_count >= 7
ORDER BY cost_count DESC, id DESC
LIMIT 20
