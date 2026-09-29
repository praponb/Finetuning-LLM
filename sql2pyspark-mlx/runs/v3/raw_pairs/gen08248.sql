WITH valid_shipments AS (
    SELECT *, TRIM(status) AS clean_status
    FROM shipments
    WHERE order_id IN (6, 22, 38) OR cost BETWEEN 67 AND 68
),
carrier_summary AS (
    SELECT carrier, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM valid_shipments
    GROUP BY carrier
)
SELECT *
FROM carrier_summary
