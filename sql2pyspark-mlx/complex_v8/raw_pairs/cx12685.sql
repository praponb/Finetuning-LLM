WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        site_id,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, burst_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_site_id_burst_id_by_session AS session_end_rank,
        COUNT(*) OVER w_session_end_to_date AS rows_in_session_end_to_date,
        SUM(n_events) OVER (PARTITION BY n_events, TO_DATE(session_start) ORDER BY n_events DESC, site_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_events,
        LEAD(n_events) OVER (PARTITION BY site_id, burst_id ORDER BY session_end DESC NULLS FIRST, site_id, burst_id) AS next_n_events
    FROM per_session
    WINDOW w_site_id_burst_id_by_session AS (PARTITION BY site_id, burst_id ORDER BY session_end DESC NULLS FIRST, site_id, burst_id), w_session_end_to_date AS (PARTITION BY TO_DATE(session_end))
)
-- Native SQL UNPIVOT (wide to long)
SELECT site_id, burst_id, field, amount
FROM (
    SELECT site_id, burst_id, rows_in_session_end_to_date, running_n_events, n_events, next_n_events
    FROM with_trends
) src
UNPIVOT (
    amount FOR field IN (rows_in_session_end_to_date AS rows_in_session_end_to_date_v, running_n_events AS running_n_events_v, n_events AS m_n_events, next_n_events AS m_next_n_events)
) unp
ORDER BY site_id, burst_id, field
