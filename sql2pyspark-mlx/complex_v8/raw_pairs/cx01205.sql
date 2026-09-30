WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-14'
      AND ticket_id >= 51
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.ticket_requester_id,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id_2,
        t.opened_at AS ticket_opened_at
    FROM joined_ticket s
    JOIN tickets t ON t.id = s.ticket_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(ticket_requester_id_2 / NULLIF(ticket_requester_id, 0), 3) AS ticket_requester_id_2_per_ticket_requester_id,
        ROUND(ticket_requester_id / NULLIF(ticket_requester_id_2, 0), 3) AS ticket_requester_id_per_ticket_requester_id_2,
        GREATEST(ticket_requester_id_2, ticket_requester_id) AS greatest_ticket_requester_id_2_ticket_requester_
    FROM with_parent_ticket
    WHERE event_at < TIMESTAMP '2025-02-18 12:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_priority,
        s.ticket_opened_at,
        s.ticket_requester_id_2_per_ticket_requester_id,
        s.ticket_requester_id_per_ticket_requester_id_2,
        t.opened_at AS ticket_opened_at_2,
        t.priority AS ticket_priority_2,
        su.tier AS agent_tier
    FROM scored s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2
    FROM joined s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    ticket_priority,
    ticket_opened_at,
    ticket_requester_id_2_per_ticket_requester_id,
    ticket_requester_id_per_ticket_requester_id_2,
    ticket_opened_at_2,
    ticket_priority_2,
    agent_tier,
    first_event_at_2,
    LOWER(REGEXP_REPLACE(TRIM(ticket_priority_2), ' ', '_')) AS ticket_priority_2_slug
FROM with_stats
ORDER BY ticket_priority_2 DESC, event_at
