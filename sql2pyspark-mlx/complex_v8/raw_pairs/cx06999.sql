WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE kwh >= 42.1
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        site_id,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, burst_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        burst_id,
        events,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        COUNT(*) AS row_count
    FROM per_session
    GROUP BY burst_id, events
    HAVING AVG(session_voltage) > 518.0
)
-- Final projection
SELECT burst_id, distinct_site_ids, row_count
FROM grouped
ORDER BY row_count DESC
