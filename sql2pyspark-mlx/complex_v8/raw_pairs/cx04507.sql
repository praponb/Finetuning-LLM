WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE (ticket_id <= 57 OR event_at >= TIMESTAMP '2025-01-25 10:00:00')
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_ref,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        su.team AS agent_team
    FROM base s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id
    FROM with_details_ticket s
    JOIN support_agents su ON su.id = s.ticket_agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, ticket_requester_id, ticket_csat
    FROM with_parent_agent
) src
UNPIVOT (
    reading FOR metric IN (ticket_requester_id AS m_ticket_requester_id, ticket_csat AS ticket_csat_v)
) unp
ORDER BY id, metric
