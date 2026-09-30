WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label_2
    FROM base
    WHERE event_at >= TIMESTAMP '2025-01-25 08:00:00'
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_actor_label,
        s.event_at_label,
        s.event_type_actor_label_2,
        t.priority AS ticket_priority,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM derived s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.priority NOT IN ('P1', 'P2')
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_type_actor_label,
    event_at_label,
    event_type_actor_label_2,
    agent_hired_on,
    agent_name,
    LOWER(REGEXP_REPLACE(TRIM(event_type_actor_label), ' ', '_')) AS event_type_actor_label_slug
FROM with_parent_ticket
WHERE event_type NOT IN ('resolved', 'assigned')
ORDER BY event_at_label DESC, id
LIMIT 20
