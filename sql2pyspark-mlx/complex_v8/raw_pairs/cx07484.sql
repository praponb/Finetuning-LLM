WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        tags,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 3)
      AND requester_id <= 19
)
SELECT id
FROM cleaned_tickets
INTERSECT
SELECT id
FROM support_agents
ORDER BY id
