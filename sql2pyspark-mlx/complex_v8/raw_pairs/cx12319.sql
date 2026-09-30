WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('reopened', 'created') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE ticket_id <= 3
      AND actor = 'agent'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags,
        su.tier AS agent_tier
    FROM with_details s
    JOIN tickets t ON t.id = s.ticket_id AND ARRAY_CONTAINS(t.tags, 'login')
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.agent_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    ticket_requester_id,
    agent_hired_on,
    agent_team,
    ticket_channel,
    ticket_agent_id,
    ticket_tags
FROM with_parent
WHERE ticket_id <= 45
ORDER BY agent_hired_on DESC NULLS FIRST
