WITH valid_shipments AS (
    SELECT *
    FROM shipments
    WHERE cost >= 72 AND status != 'IN_TRANSIT'
),
status_summary AS (
    SELECT status, MAX(delivered_date) AS latest_delivered_date, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
    FROM valid_shipments
    GROUP BY status
)
SELECT status, latest_delivered_date
FROM status_summary;
