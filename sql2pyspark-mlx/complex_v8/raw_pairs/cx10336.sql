WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 13
),
with_prev AS (
    -- Previous read_at per quality_or_unknown
    SELECT *, LAG(read_at) OVER (PARTITION BY quality_or_unknown ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality_or_unknown ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality_or_unknown,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality_or_unknown, burst_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE duration_min > (SELECT AVG(duration_min) FROM per_session)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        quality_or_unknown,
        burst_id,
        session_start,
        session_end,
        n_events,
        duration_min,
        DENSE_RANK() OVER (ORDER BY session_end, quality_or_unknown, burst_id) AS session_end_dense_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_session_end_ties, 3) AS session_end_percent_rank,
        SIZE(COLLECT_SET(quality_or_unknown) OVER (PARTITION BY n_events)) AS distinct_quality_or_unknown_in_n_events,
        RANK() OVER (PARTITION BY quality_or_unknown, burst_id ORDER BY session_start ASC, quality_or_unknown, burst_id) AS session_start_rank,
        SUM(burst_id) OVER (PARTITION BY n_events, burst_id ORDER BY duration_min DESC, quality_or_unknown, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id,
        ROUND(n_events - AVG(n_events) OVER (PARTITION BY n_events), 2) AS n_events_vs_n_events_avg
    FROM with_activity
    WINDOW w_all_by_session_end_ties AS (ORDER BY session_end)
)
-- Native SQL UNPIVOT (wide to long)
SELECT quality_or_unknown, burst_id, measure, reading
FROM (
    SELECT quality_or_unknown, burst_id, distinct_quality_or_unknown_in_n_events, running_burst_id, session_end_dense_rank, n_events
    FROM windowed
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (distinct_quality_or_unknown_in_n_events, running_burst_id, session_end_dense_rank, n_events)
) unp
