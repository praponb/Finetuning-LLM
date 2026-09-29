WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        event_type,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_no
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_session_no AS rows_in_session_no,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY session_no), 0), 4) AS session_no_share_of_session_no,
        DENSE_RANK() OVER w_all_by_session_end_ties AS session_end_dense_rank
    FROM per_session
    WINDOW w_session_no AS (PARTITION BY session_no), w_all_by_session_end_ties AS (ORDER BY session_end)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        event_type,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        COUNT(*) FILTER (WHERE event_type NOT IN ('resolved', 'reopened')) AS n_event_type_not_in_resolved_r,
        ROUND(SUM(session_no_share_of_session_no) / NULLIF(SUM(session_end_dense_rank), 0), 3) AS session_no_share_of_session_no_to_session_end_de,
        SUM(CASE WHEN event_type IS NOT NULL THEN 1 ELSE 0 END) AS n_event_type_is_not_null,
        SUM(duration_min) AS total_duration_min
    FROM with_windows
    GROUP BY event_type, DATE_TRUNC('MONTH', session_start)
)
-- Final projection
SELECT event_type, n_event_type_is_not_null
FROM rollup_base
