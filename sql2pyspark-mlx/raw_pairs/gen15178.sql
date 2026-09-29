WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE cost <= 37
),
agg AS (
    SELECT status, MIN(cost) AS min_cost
    FROM recent_shipments
    GROUP BY status
)
SELECT *
FROM agg;
