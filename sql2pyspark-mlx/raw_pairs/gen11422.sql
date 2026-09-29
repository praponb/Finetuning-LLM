WITH ValidShipments AS (
    SELECT *, DAY(shipped_date) shipped_date_day
    FROM shipments
    WHERE order_id IN (30, 31, 33, 34)
),
delivered_month_summary AS (
    SELECT DATE_FORMAT(delivered_date, 'yyyy-MM') delivered_month, SORT_ARRAY(COLLECT_SET(status)) status_list
    FROM ValidShipments
    GROUP BY DATE_FORMAT(delivered_date, 'yyyy-MM')
)
SELECT delivered_month, status_list
FROM delivered_month_summary
ORDER BY delivered_month;
