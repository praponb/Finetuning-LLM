SELECT ticket_id, id AS entity_id, 'event' AS src
FROM ticket_events
UNION ALL
SELECT csat, id, 'ticket' AS src
FROM tickets
WHERE opened_at < TIMESTAMP '2025-02-18 13:00:00'
ORDER BY ticket_id DESC
