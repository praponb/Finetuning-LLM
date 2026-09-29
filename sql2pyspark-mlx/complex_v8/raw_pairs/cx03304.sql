WITH joined_ranges AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.agent_id,
        s.hours_since_open,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM joined_ranges s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(event_event_type)) AS event_event_type_clean,
        COALESCE(event_actor, 'UNKNOWN') AS event_actor_or_unknown
    FROM with_children_events
    WHERE MONTH(event_event_at) IN (1, 3)
)
-- Final projection
SELECT
    id,
    event_id,
    event_actor,
    event_event_type,
    event_event_type_clean
FROM annotated
