WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        site_id,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, burst_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(event_count) OVER w_event_count, 2) AS event_count_avg_event_count,
        ROUND(PERCENT_RANK() OVER w_session_start_to_date_by_ses, 3) AS session_end_percent_rank,
        SUM(duration_min) OVER (PARTITION BY TO_DATE(session_start) ORDER BY session_end ASC, site_id, burst_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_min_sum_7
    FROM sessions
    WINDOW w_event_count AS (PARTITION BY event_count), w_session_start_to_date_by_ses AS (PARTITION BY TO_DATE(session_start) ORDER BY session_end ASC)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 3600, 1) AS hours_session_end_to_session_start,
        ROUND(duration_min_sum_7 * 1.07, 2) AS duration_min_sum_7_adj,
        CASE WHEN session_end_percent_rank >= 0.8 THEN 'HIGH' WHEN session_end_percent_rank >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END AS session_end_percent_rank_tier
    FROM with_trends
)
SELECT *
FROM (
    SELECT burst_id, session_end_percent_rank_tier, event_count
    FROM labeled
) src
PIVOT (
    SUM(event_count) AS amount, MAX(event_count) AS max_v
    FOR session_end_percent_rank_tier IN ('LOW', 'HIGH')
)
ORDER BY burst_id
