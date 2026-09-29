WITH recent_shipments AS (
    SELECT *, CEIL(cost) AS cost_whole
    FROM shipments
    WHERE carrier IN ('Flash', 'UPS')
),
summary AS (
    SELECT order_id, MIN(shipped_date) AS min_shipped_date
    FROM recent_shipments
    GROUP BY order_id
)
SELECT order_id, min_shipped_date
FROM summary;
