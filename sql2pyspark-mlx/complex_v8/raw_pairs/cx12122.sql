WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, burst_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    events,
    burst_id,
    GROUPING(quality) AS is_total_quality,
    ROUND(STDDEV(events), 2) AS stddev_events,
    COUNT(*) FILTER (WHERE quality NOT IN ('EST', 'BAD')) AS n_quality_not_in_est_bad,
    SUM(events) FILTER (WHERE quality IN ('OK', 'EST')) AS events_quality_in_ok_est
FROM per_session
GROUP BY GROUPING SETS ((quality, events, burst_id), (quality), (events), ())
ORDER BY quality NULLS LAST, events NULLS LAST, burst_id NULLS LAST
