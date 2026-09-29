WITH recent_shipments AS (
    SELECT *, CEIL(cost) AS cost_whole
    FROM shipments
    WHERE carrier IN ('Flash', 'UPS')
),
status_summary AS (
    SELECT status, COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count
    FROM recent_shipments
    GROUP BY status
)
SELECT status, in_transit_count
FROM status_summary
ORDER BY status;
