SELECT
    'ticket' AS record_type,
    id AS qty,
    channel,
    agent_id
FROM tickets
WHERE priority <> 'P3'
  AND channel <> 'email'
UNION ALL
SELECT
    'event' AS record_type,
    id,
    event_type,
    ticket_id
FROM ticket_events
ORDER BY channel DESC
