WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat
    FROM tickets
    WHERE csat < 4
)
SELECT id, agent_id, channel
FROM base_tickets
WHERE agent_id IS NOT NULL
ORDER BY id, agent_id
