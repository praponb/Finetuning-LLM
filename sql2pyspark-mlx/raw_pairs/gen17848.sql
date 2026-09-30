WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY status, carrier ORDER BY cost DESC NULLS LAST) AS row_num
    FROM shipments
    QUALIFY row_num = 1
)
SELECT
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE status = 'LOST') AS lost_rows,
    COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered_rows,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN status = 'RETURNED' THEN 1 END), 0), 2) AS cost_per_returned
FROM ranked
GROUP BY 1;
