WITH RecentShipments AS (
    SELECT *, COALESCE(carrier, 'unassigned') AS carrier_filled
    FROM shipments
    WHERE status IN ('IN_TRANSIT', 'RETURNED', 'DELIVERED')
),
agg AS (
    SELECT order_id, SUM(CASE WHEN status = 'DELIVERED' THEN cost ELSE 0 END) AS delivered_cost, MAX(cost) - MIN(cost) AS cost_range
    FROM RecentShipments
    GROUP BY order_id
)
SELECT *
FROM agg
WHERE delivered_cost < 32
ORDER BY cost_range;
