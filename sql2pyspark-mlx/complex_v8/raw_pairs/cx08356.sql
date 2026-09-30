WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'BAD'
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        site_id,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(session_id / NULLIF(duration_min, 0), 3) AS session_id_per_duration_min,
        ROUND(duration_min * 1.5, 2) AS duration_min_adj,
        DATEDIFF(DATE '2025-12-31', session_start) AS days_since_session_start
    FROM sessions
)
-- Final projection
SELECT
    site_id,
    session_id,
    session_start,
    session_end,
    event_count,
    session_voltage,
    duration_min,
    duration_min_adj,
    days_since_session_start,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 3600, 1) AS hours_session_end_to_session_start
FROM tagged
WHERE event_count BETWEEN 2 AND 5
ORDER BY session_start, session_voltage
