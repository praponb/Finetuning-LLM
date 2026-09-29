WITH base AS (
    SELECT *, DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date
    FROM shipments
    WHERE delivered_date <= '2025-04-23' AND shipped_date < '2023-07-01'
),
shipments_stats AS (
    SELECT
        status,
        carrier,
        SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END) AS flash_count,
        MAX(days_shipped_date_to_delivered_date) - MIN(days_shipped_date_to_delivered_date) AS days_shipped_date_to_delivered_date_range
    FROM base
    GROUP BY status, carrier
)
SELECT status, carrier, days_shipped_date_to_delivered_date_range
FROM shipments_stats
ORDER BY status DESC;
