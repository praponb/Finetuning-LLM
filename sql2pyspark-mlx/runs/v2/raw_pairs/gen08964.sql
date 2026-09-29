WITH filtered_shipments AS (
    SELECT *
    FROM shipments
    WHERE status <> 'IN_TRANSIT'
),
shipped_month_summary AS (
    SELECT MONTH(shipped_date) AS shipped_month, COUNT(*) AS cnt, MAX(cost) AS max_cost
    FROM filtered_shipments
    GROUP BY MONTH(shipped_date)
)
SELECT shipped_month, cnt, max_cost
FROM shipped_month_summary
ORDER BY shipped_month, max_cost DESC;
