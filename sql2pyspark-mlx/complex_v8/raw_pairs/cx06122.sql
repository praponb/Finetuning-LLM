WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE event_type = 'assigned'
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_or_unknown,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
),
latest AS (
    -- Keep the latest row per actor, ticket_agent_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_or_unknown,
        ticket_closed_at,
        ticket_opened_at,
        ticket_agent_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_agent_id ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM combined_details
    ) ranked
    WHERE recency_rank = 1
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.ticket_closed_at,
        s.ticket_agent_id,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM latest s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_at >= TIMESTAMP '2025-01-25 13:00:00'
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.ticket_closed_at,
        s.ticket_agent_id,
        s.event_id,
        s.event_event_at,
        s.event_actor,
        t.opened_at AS ticket_opened_at,
        t.priority AS ticket_priority,
        ti.event_at,
        ti.actor AS event_actor_2
    FROM exploded_detail s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN ticket_events ti ON ti.id = s.event_id
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    ticket_closed_at,
    ticket_agent_id,
    event_id,
    event_event_at,
    event_actor,
    ticket_opened_at,
    ticket_priority,
    event_actor_2,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM with_details_ticket
WHERE TO_DATE(ticket_opened_at) < DATE '2025-02-12'
ORDER BY event_actor_2 DESC, ticket_priority DESC NULLS FIRST, id, event_id
LIMIT 20
