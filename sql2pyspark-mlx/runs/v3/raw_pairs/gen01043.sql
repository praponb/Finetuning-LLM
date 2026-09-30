WITH base AS (
    SELECT *, MONTH(delivered_date) AS delivered_date_month
    FROM shipments
    WHERE MONTH(delivered_date) IN (2, 8, 12) AND order_id = 3
),
shipments_stats AS (
    SELECT carrier, CASE WHEN delivered_date_month >= 65 THEN 'high' WHEN delivered_date_month >= 22 THEN 'mid' ELSE 'low' END AS delivered_date_month_bucket, ROUND((100 * SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_flash
    FROM base
    GROUP BY carrier, CASE WHEN delivered_date_month >= 65 THEN 'high' WHEN delivered_date_month >= 22 THEN 'mid' ELSE 'low' END
)
SELECT carrier, pct_flash
FROM shipments_stats
