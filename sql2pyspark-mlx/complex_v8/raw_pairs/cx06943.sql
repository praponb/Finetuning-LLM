WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE actor = 'agent'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM recent_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_actor_label,
        s.prev_event_at,
        s.is_new_session,
        s.session_id,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM sessionized s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
unique_rows AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_type_actor_label,
        prev_event_at,
        is_new_session,
        session_id,
        event_id,
        event_event_at,
        event_actor,
        event_event_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_event_at DESC, id DESC, event_id DESC) AS recency_rank
        FROM line_level
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    actor,
    event_type_actor_label,
    is_new_session,
    session_id,
    event_id,
    event_event_at,
    event_actor,
    event_event_type,
    QUARTER(prev_event_at) AS prev_event_at_quarter
FROM unique_rows
ORDER BY event_id, prev_event_at_quarter
