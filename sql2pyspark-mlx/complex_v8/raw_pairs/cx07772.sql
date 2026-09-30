WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 233.1
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM filtered_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        quality,
        session_id,
        session_start,
        session_end,
        NTH_VALUE(session_start, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', session_start) ORDER BY session_end ASC, quality, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_start,
        COUNT(*) OVER (PARTITION BY session_id) AS rows_in_session_id,
        ROUND(CUME_DIST() OVER (ORDER BY duration_min DESC, quality, session_id), 3) AS duration_min_cume_dist,
        duration_min - LAG(duration_min) OVER (ORDER BY duration_min DESC, quality, session_id) AS duration_min_change
    FROM per_session
)
SELECT
    quality,
    session_id,
    session_start,
    session_end,
    second_session_start,
    rows_in_session_id,
    duration_min_cume_dist,
    duration_min_change,
    ABS(session_id - 4) AS session_id_distance,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(second_session_start)) / 60, 1) AS minutes_second_session_start_to_session_start
FROM metrics
ORDER BY session_id
