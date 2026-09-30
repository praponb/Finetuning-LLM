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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM joined_ranges s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type NOT IN ('resolved', 'created')
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.event_id,
        s.event_event_type,
        s.event_actor,
        s.event_event_at,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at
    FROM exploded_detail s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with CUBE
SELECT
    event_actor,
    event_id,
    ARRAY_SORT(COLLECT_SET(event_actor)) AS event_actor_list
FROM with_parent
GROUP BY CUBE(event_actor, event_id)
ORDER BY event_actor NULLS LAST, event_id NULLS LAST
