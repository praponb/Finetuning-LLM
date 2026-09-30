WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE YEAR(delivered_date) = 2024
),
shipments_stats AS (
    SELECT status, carrier, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM recent_shipments
    GROUP BY status, carrier
)
SELECT status, status_values
FROM shipments_stats
ORDER BY status_values, status
