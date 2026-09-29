WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Subtotals with GROUPING SETS
SELECT
    session_id,
    is_new_session,
    site_id,
    MIN(read_at) AS first_read_at,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM sessionized
GROUP BY GROUPING SETS ((session_id, is_new_session, site_id), (session_id), (is_new_session))
ORDER BY session_id NULLS LAST, is_new_session NULLS LAST, site_id NULLS LAST
