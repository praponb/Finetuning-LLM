WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM scoped_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_count, quality ORDER BY duration_min, quality, session_no), 3) AS duration_min_percent_rank,
        MAX(duration_min) OVER (PARTITION BY event_count, quality) AS event_count_quality_max_duration_min,
        RANK() OVER w_quality_by_session_end AS session_end_rank,
        ROUND(session_voltage / NULLIF(SUM(session_voltage) OVER (PARTITION BY event_count, quality), 0), 4) AS session_voltage_share_of_event_count_quality
    FROM per_session
    WINDOW w_quality_by_session_end AS (PARTITION BY quality ORDER BY session_end, quality, session_no)
)
SELECT
    quality,
    session_no,
    session_start,
    event_count,
    session_voltage,
    duration_min,
    duration_min_percent_rank,
    event_count_quality_max_duration_min,
    session_end_rank,
    session_voltage_share_of_event_count_quality
FROM metrics
