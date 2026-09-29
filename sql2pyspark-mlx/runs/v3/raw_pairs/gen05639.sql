WITH ValidShipments AS (
    SELECT *
    FROM shipments
    WHERE (order_id IS NOT NULL AND MONTH(shipped_date) IN (1, 7, 9)) OR order_id = 13
),
agg AS (
    SELECT status, MAX(cost) - MIN(cost) AS cost_range
    FROM ValidShipments
    GROUP BY status
)
SELECT status, cost_range
FROM agg
WHERE cost_range < 73
ORDER BY status, cost_range DESC;
