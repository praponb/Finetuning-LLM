WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM ticket_events
    WHERE actor <> 'system'
      AND ticket_id >= 32
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_year,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id,
        su.tier AS agent_tier
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
ticket_id_stats AS (
    -- Max ticket_requester_id per ticket_id
    SELECT ticket_id, MAX(ticket_requester_id) AS max_ticket_requester_id
    FROM with_parent
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ticket_requester_id,
        ROUND(s.ticket_requester_id - g.max_ticket_requester_id, 2) AS ticket_requester_id_minus_max
    FROM with_parent s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.ticket_requester_id = g.max_ticket_requester_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_tier, ticket_priority, ticket_requester_id
    FROM above_average
) src
PIVOT (
    SUM(ticket_requester_id)
    FOR ticket_priority IN ('P3', 'P1', 'P2', 'P4')
)
ORDER BY agent_tier
