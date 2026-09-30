SELECT
    priority AS label,
    closed_at AS event_time,
    id AS entity_id,
    'ticket' AS origin
FROM tickets
UNION ALL
SELECT
    channel,
    opened_at,
    agent_id,
    'ticket_2' AS origin
FROM tickets
ORDER BY origin, label
