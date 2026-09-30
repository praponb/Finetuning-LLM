WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day
    FROM ticket_events
    WHERE event_type <> 'escalated'
      AND actor IN ('agent', 'system')
)
SELECT 'event' AS origin, id AS entity_id, actor AS name
FROM cleaned
WHERE HOUR(event_at_day) BETWEEN 9 AND 14
UNION ALL
SELECT 'ticket' AS origin, requester_id AS entity_id, priority AS name
FROM tickets
WHERE requester_id >= 8
UNION ALL
SELECT 'agent' AS origin, id AS entity_id, team AS name
FROM support_agents
WHERE team = 'Onboarding'
  AND UPPER(TRIM(agent_name)) LIKE 'K%'
