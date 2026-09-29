WITH filtered_shipments AS (
    SELECT *, CASE WHEN cost >= 105 THEN 'High' WHEN cost >= 26 THEN 'Medium' ELSE 'Low' END cost_band
    FROM shipments
    WHERE status <> 'IN_TRANSIT'
),
shipments_stats AS (
    SELECT carrier, COUNT(DISTINCT cost) unique_orders
    FROM filtered_shipments
    GROUP BY carrier
)
SELECT carrier, unique_orders
FROM shipments_stats
ORDER BY carrier;
