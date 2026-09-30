WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM cleaned_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
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
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_no
),
labeled AS (
    -- Add calculated columns
    SELECT
        quality,
        session_no,
        session_start,
        session_end,
        n_events,
        duration_min,
        ROUND(duration_min * 1.1, 2) AS duration_min_adj,
        ROUND(duration_min * 1.07, 2) AS duration_min_adj_2,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM sessions
    WHERE TO_DATE(session_start) < DATE '2025-02-05'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(duration_min_adj, duration_min_adj_2) AS least_duration_min_adj_duration_min_adj_2,
        n_events - duration_min AS n_events_minus_duration_min,
        DATE_TRUNC('DAY', session_end) AS session_end_day
    FROM labeled
    WHERE MONTH(session_start) IN (1, 2)
)
-- Native SQL UNPIVOT (wide to long)
SELECT quality, session_no, field, metric_value
FROM (
    SELECT quality, session_no, duration_min_adj_2, least_duration_min_adj_duration_min_adj_2, duration_min_adj, n_events_minus_duration_min
    FROM tagged
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (duration_min_adj_2, least_duration_min_adj_duration_min_adj_2, duration_min_adj, n_events_minus_duration_min)
) unp
ORDER BY quality, session_no, field
