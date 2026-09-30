WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE carrier <> 'Kerry'
),
shipments_stats AS (
    SELECT status, MIN(cost) AS min_cost, MAX(cost) AS max_cost
    FROM recent_shipments
    GROUP BY status
)
SELECT *
FROM shipments_stats
WHERE max_cost < 63;
