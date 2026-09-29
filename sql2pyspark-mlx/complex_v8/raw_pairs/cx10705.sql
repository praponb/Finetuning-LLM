WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.prev_event_at,
        s.session_no,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM sessionized s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        session_no,
        event_id,
        event_event_at,
        event_event_type,
        COUNT(*) OVER w_ticket_id_actor AS rows_in_ticket_id_actor
    FROM with_children_events
    WINDOW w_ticket_id_actor AS (PARTITION BY ticket_id, actor)
)
-- Final projection
SELECT
    id,
    event_at,
    session_no,
    event_id,
    event_event_type
FROM sequenced
ORDER BY id DESC, event_event_type
