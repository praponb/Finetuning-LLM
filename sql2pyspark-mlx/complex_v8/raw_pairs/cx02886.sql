WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE quality = 'OK'
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM prepared_readings
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
sessions AS (
    -- One row per session
    SELECT
        quality,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        quality,
        n_events,
        session_no,
        PERCENTILE_APPROX(session_kwh, 0.5) AS median_session_kwh,
        COUNT(*) FILTER (WHERE quality NOT IN ('BAD', 'EST')) AS n_quality_not_in_bad_est,
        COUNT(*) FILTER (WHERE quality = 'OK') AS n_quality_ok,
        COUNT(*) AS row_count,
        MAX(n_events) AS max_n_events,
        ROUND(SUM(duration_min) / NULLIF(SUM(n_events), 0), 3) AS duration_min_to_n_events_ratio
    FROM sessions
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_n_events_by_row_count AS rn,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY session_no), 0), 4) AS session_no_share_of_session_no,
        FIRST_VALUE(n_quality_not_in_bad_est) OVER (PARTITION BY n_events ORDER BY row_count, quality, n_events, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_quality_not_in_bad_est,
        SUM(max_n_events) OVER (PARTITION BY n_events ORDER BY row_count, quality, n_events, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_n_events,
        COUNT(*) OVER (PARTITION BY session_no) AS rows_in_session_no
    FROM stats
    WINDOW w_n_events_by_row_count AS (PARTITION BY n_events ORDER BY row_count, quality, n_events, session_no)
)
-- Final projection
SELECT
    quality,
    n_events,
    n_quality_ok,
    row_count,
    max_n_events,
    duration_min_to_n_events_ratio,
    rn,
    first_n_quality_not_in_bad_est,
    running_max_n_events
FROM metrics
ORDER BY rn, n_quality_ok DESC NULLS FIRST
