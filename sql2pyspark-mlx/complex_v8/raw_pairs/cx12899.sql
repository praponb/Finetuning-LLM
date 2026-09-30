WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE ticket_id >= 45
)
SELECT actor, id
FROM recent
WHERE ticket_id IS NOT NULL
  AND event_type <> 'replied'
UNION
SELECT agent_name, id
FROM support_agents
WHERE team IS NOT NULL
ORDER BY id
