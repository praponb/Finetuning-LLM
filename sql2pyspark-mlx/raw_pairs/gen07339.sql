WITH recent_shipments AS (
    SELECT *
    FROM shipments
    WHERE status <> 'DELIVERED'
),
shipped_quarter_summary AS (
    SELECT QUARTER(shipped_date) AS shipped_quarter, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
    FROM recent_shipments
    GROUP BY QUARTER(shipped_date)
)
SELECT shipped_quarter, carrier_list
FROM shipped_quarter_summary
ORDER BY 1 DESC;
