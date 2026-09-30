WITH base AS (
    SELECT *, DATE_TRUNC('YEAR', shipped_date) AS shipped_date_period
    FROM shipments
    WHERE shipped_date BETWEEN '2023-08-07' AND '2025-09-12' AND delivered_date < '2023-05-15'
),
status_summary AS (
    SELECT status, MAX(cost) AS max_cost, MAX(cost) - MIN(cost) AS cost_range
    FROM base
    GROUP BY status
)
SELECT status, max_cost
FROM status_summary
ORDER BY max_cost
