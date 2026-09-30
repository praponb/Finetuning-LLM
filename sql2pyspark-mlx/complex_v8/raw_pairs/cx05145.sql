WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 14:00:00'
      AND ticket_id >= 56
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.days_since_event_at,
        t.requester_id AS ticket_requester_id,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_requester_id,
        agent_name,
        DATEDIFF(DATE '2026-01-01', agent_hired_on) AS days_since_agent_hired_on,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        ROUND(days_since_event_at / NULLIF(ticket_requester_id, 0), 3) AS days_since_event_at_per_ticket_requester_id
    FROM combined_details
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id_2,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id AND t.csat < 4
    WHERE TO_DATE(t.closed_at) < DATE '2025-02-27'
)
SELECT id, metric, value
FROM (
    SELECT id, ticket_requester_id, days_since_agent_hired_on, ticket_requester_id_2
    FROM with_details_ticket
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (ticket_requester_id AS ticket_requester_id_v, days_since_agent_hired_on AS days_since_agent_hired_on_v, ticket_requester_id_2 AS m_ticket_requester_id_2)
) unp
