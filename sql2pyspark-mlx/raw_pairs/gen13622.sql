WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE status IN ('IN_TRANSIT', 'DELIVERED') AND order_id = 5
),
summary AS (
    SELECT status, ROUND(AVG(cost), 2) avg_cost
    FROM recent_shipments
    GROUP BY status
)
SELECT status, avg_cost
FROM summary
ORDER BY status DESC, avg_cost DESC
LIMIT 5;
