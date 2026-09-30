WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE delivered_date >= '2025-05-01' AND order_id IS NULL
),
agg AS (
    SELECT status, SORT_ARRAY(COLLECT_SET(carrier)) carrier_list, MIN(delivered_date) first_delivered_date
    FROM recent_shipments
    GROUP BY status
)
SELECT *
FROM agg
ORDER BY first_delivered_date, status
LIMIT 10
