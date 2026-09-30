WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_type_ref,
        t.agent_id AS ticket_agent_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    ticket_id,
    agent_tier,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    ROUND(100 * COUNT(CASE WHEN event_type = 'reopened' THEN 1 END) / COUNT(*), 1) AS pct_event_type_reopened
FROM with_details
GROUP BY CUBE(ticket_id, agent_tier)
ORDER BY ticket_id NULLS LAST, agent_tier NULLS LAST
