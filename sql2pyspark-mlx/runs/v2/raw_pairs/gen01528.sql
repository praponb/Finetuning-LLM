WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE cost <= 75 AND status IN ('IN_TRANSIT', 'RETURNED') AND order_id IS NOT NULL
),
agg AS (
    SELECT
        carrier,
        QUARTER(delivered_date) delivered_quarter,
        COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) returned_count,
        COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END) kerry_count
    FROM recent_shipments
    GROUP BY carrier, QUARTER(delivered_date)
)
SELECT
    carrier,
    delivered_quarter,
    returned_count,
    kerry_count
FROM agg
ORDER BY delivered_quarter
