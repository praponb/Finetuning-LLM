SELECT
    priority AS label,
    id AS key_id,
    channel,
    'ticket' AS src
FROM tickets
WHERE requester_id < 15
UNION ALL
SELECT
    actor,
    id,
    event_type,
    'event' AS src
FROM ticket_events
ORDER BY label, key_id
