WITH base AS (
    SELECT *, MONTH(delivered_date) AS delivered_date_month
    FROM shipments
    WHERE MONTH(delivered_date) IN (2, 8, 12) AND order_id = 3
),
shipments_stats AS (
    SELECT carrier, status, MAX(cost) AS max_cost
    FROM base
    GROUP BY carrier, status
)
SELECT carrier, status, max_cost
FROM shipments_stats
