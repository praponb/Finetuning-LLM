WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(status) clean_status,
        COALESCE(method, 'N/A') method_clean
    FROM payments
    WHERE method IN ('card', 'bank_transfer')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    MAX(paid_at) AS last_seen,
    ROUND(AVG(session_id), 2) AS avg_session_id
FROM sessionized
GROUP BY DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('YEAR', paid_at);
