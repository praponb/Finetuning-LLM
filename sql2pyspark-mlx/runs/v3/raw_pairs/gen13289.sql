WITH valid_shipments AS (
    SELECT *
    FROM shipments
    WHERE order_id IS NULL OR status = 'DELIVERED'
),
status_summary AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM valid_shipments
    GROUP BY status
)
SELECT status, status_list, stddev_cost
FROM status_summary
WHERE stddev_cost > 45;
