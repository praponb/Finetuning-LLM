WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'agent'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_id
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_ym,
        session_id,
        n_events,
        COUNT(*) AS row_count,
        ROUND(AVG(n_events), 2) AS avg_n_events,
        COUNT(DISTINCT actor) AS distinct_actors
    FROM per_session
    GROUP BY DATE_FORMAT(session_end, 'yyyy-MM'), session_id, n_events
    HAVING MAX(duration_min) > 84.0
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM totals
    WHERE session_id > (SELECT MIN(session_id) FROM totals)
)
-- Final projection
SELECT
    session_end_ym,
    n_events,
    avg_n_events,
    ROUND(avg_n_events * 1.07, 2) AS avg_n_events_adj,
    row_count + distinct_actors AS row_count_plus_distinct_actors
FROM screened
WHERE session_end_ym IS NOT NULL
