WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE (priority NOT IN ('P4', 'P2') OR csat BETWEEN 1 AND 4)
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.requester_id_plus_csat,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.agent_team,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM combined_details s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_details AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.agent_team,
        s.event_id,
        s.event_at,
        s.event_actor,
        s.event_type,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM detail_rows_events s
    JOIN ticket_events t ON t.id = s.event_id
    WHERE t.event_type IN ('replied', 'escalated')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_event_type, priority, csat
    FROM with_details
) src
PIVOT (
    COUNT(csat)
    FOR priority IN ('P3', 'P2')
)
ORDER BY event_event_type
