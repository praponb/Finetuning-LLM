WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
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
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(n_events - AVG(n_events) OVER (PARTITION BY event_type, MONTH(session_start)), 2) AS n_events_vs_event_type_session_start_month_avg,
        ROUND(PERCENT_RANK() OVER w_event_type_session_no_by_ses, 3) AS session_start_percent_rank,
        ROUND(session_no / NULLIF(SUM(session_no) OVER w_event_type_session_start_mon, 0), 4) AS session_no_share_of_event_type_session_start_mon
    FROM sessions
    WINDOW w_event_type_session_start_mon AS (PARTITION BY event_type, MONTH(session_start)), w_event_type_session_no_by_ses AS (PARTITION BY event_type, session_no ORDER BY session_start DESC)
)
SELECT
    session_no,
    COALESCE(event_type, 'ALL') AS event_type,
    PERCENTILE_APPROX(session_no, 0.5) AS median_session_no,
    COUNT(DISTINCT event_type) AS distinct_event_types
FROM ranked
GROUP BY ROLLUP(session_no, event_type)
ORDER BY session_no NULLS LAST, event_type NULLS LAST
