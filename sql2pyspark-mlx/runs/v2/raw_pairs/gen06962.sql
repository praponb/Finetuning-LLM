WITH filtered_shipments AS (
    SELECT *
    FROM shipments
    WHERE cost > 50 AND order_id IS NULL
),
summary AS (
    SELECT status, COUNT(DISTINCT cost) unique_carriers, COUNT(cost) cost_count
    FROM filtered_shipments
    GROUP BY status
)
SELECT status, unique_carriers, cost_count
FROM summary;
