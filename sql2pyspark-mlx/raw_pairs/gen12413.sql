WITH recent_shipments AS (
    SELECT *, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
    FROM shipments
    WHERE order_id IS NULL
),
summary AS (
    SELECT carrier, MIN(days_delivered_date_to_shipped_date) AS min_days_delivered_date_to_shipped_date
    FROM recent_shipments
    GROUP BY carrier
)
SELECT carrier, min_days_delivered_date_to_shipped_date
FROM summary
ORDER BY carrier
