WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        ticket_id,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_id
),
line_level_events AS (
    -- One row per event
    SELECT
        s.ticket_id,
        s.session_id,
        s.session_start,
        s.session_end,
        s.n_events,
        s.duration_min,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM per_session s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('YEAR', event_at) AS event_at_year,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(session_end)) / 60, 1) AS minutes_session_end_to_event_at,
        CONCAT(event_actor, '#', CAST(ticket_id AS STRING)) AS event_actor_ref
    FROM line_level_events
)
SELECT
    ticket_id,
    session_id,
    session_start,
    n_events,
    duration_min,
    event_id,
    event_type,
    event_at,
    event_actor,
    event_actor_ref
FROM scored
WHERE ticket_id IS NOT NULL
ORDER BY ticket_id DESC NULLS FIRST
