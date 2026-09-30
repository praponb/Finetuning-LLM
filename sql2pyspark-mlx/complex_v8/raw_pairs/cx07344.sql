WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE actor = 'agent'
      AND ticket_id >= 59
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM recent_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'agent'
              AND t.event_at < TIMESTAMP '2025-02-18 12:00:00'
        )
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_event_type_label,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM qualified s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type = 'reopened'
),
with_details AS (
    -- Attach ticket attributes
    SELECT s.*, t.tags AS ticket_tags, su.hired_on AS agent_hired_on
    FROM line_level_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_id,
        s.event_event_type,
        s.event_actor,
        s.ticket_tags,
        s.agent_hired_on,
        t.id AS event_id_2,
        t.actor AS event_actor_2,
        t.event_type AS event_event_type_2
    FROM with_details s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_id,
    event_actor,
    ticket_tags,
    agent_hired_on,
    event_id_2,
    event_actor_2,
    event_event_type_2
FROM exploded_detail
ORDER BY event_actor
