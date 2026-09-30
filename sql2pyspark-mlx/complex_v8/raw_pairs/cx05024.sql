WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 5
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
scored AS (
    -- Derive helper columns
    SELECT
        quality,
        session_no,
        session_end,
        session_kwh,
        duration_min,
        DATEDIFF(DATE '2025-12-31', session_start) AS days_since_session_start
    FROM session_summary
    WHERE quality NOT IN ('BAD', 'EST')
)
SELECT
    quality,
    session_end,
    session_kwh,
    days_since_session_start,
    CONCAT(quality, '#', CAST(session_no AS STRING)) AS quality_ref,
    session_kwh + duration_min AS session_kwh_plus_duration_min
FROM scored
ORDER BY session_kwh, session_end NULLS LAST
