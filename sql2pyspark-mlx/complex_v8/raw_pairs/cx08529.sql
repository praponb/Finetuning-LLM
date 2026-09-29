WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        actor,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_no
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_events_actor_by_events_ties, 3) AS events_percent_rank,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(LAG(session_end) OVER (PARTITION BY session_no ORDER BY events, actor, session_no))) / 60, 1) AS minutes_since_prev_session_end
    FROM session_summary
    WINDOW w_events_actor_by_events_ties AS (PARTITION BY events, actor ORDER BY events DESC)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        QUARTER(session_end) AS session_end_quarter,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 3600, 1) AS hours_session_start_to_session_end
    FROM sequenced
)
-- Final projection
SELECT
    actor,
    session_no,
    session_end,
    events,
    duration_min,
    events_percent_rank,
    minutes_since_prev_session_end,
    session_end_quarter,
    hours_session_start_to_session_end,
    SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
FROM scored
ORDER BY session_end_quarter
