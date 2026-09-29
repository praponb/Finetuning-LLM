WITH recent_shipments AS (
    SELECT *, DATE_SUB(shipped_date, 78) AS shipped_date_minus_78d
    FROM shipments
    WHERE order_id = 16
),
agg AS (
    SELECT status, MAX(cost) - MIN(cost) AS cost_range, MAX(shipped_date_minus_78d) AS last_shipped_date_minus_78d
    FROM recent_shipments
    GROUP BY status
)
SELECT *
FROM agg
ORDER BY status, cost_range DESC
LIMIT 5;
