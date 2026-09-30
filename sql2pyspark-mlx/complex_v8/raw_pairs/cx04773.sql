WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        quality,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        MONTH(session_start) AS session_start_month,
        ABS(n_events - 4) AS n_events_distance
    FROM session_summary
)
-- Final projection
SELECT
    quality,
    session_id,
    session_start,
    session_end,
    n_events,
    session_kwh,
    session_start_month,
    INITCAP(TRIM(quality)) AS quality_clean,
    DATEDIFF(DATE '2025-06-30', session_start) AS days_since_session_start
FROM labeled
WHERE quality IN ('EST', 'BAD')
ORDER BY quality DESC, session_end DESC NULLS FIRST, session_id
LIMIT 10
