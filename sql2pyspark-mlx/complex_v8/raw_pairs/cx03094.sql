WITH cleaned_tickets AS (
    -- Filter tickets
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
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref
    FROM tickets
    WHERE (opened_at < TIMESTAMP '2025-03-08 10:00:00' OR channel = 'phone')
)
SELECT agent_id AS key_id, priority_ref AS category
FROM cleaned_tickets
EXCEPT
SELECT id, actor
FROM ticket_events
ORDER BY category DESC
