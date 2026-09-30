SELECT requester_id AS ref_id, closed_at, 'ticket' AS source
FROM tickets
WHERE HOUR(opened_at) BETWEEN 8 AND 10
UNION ALL
SELECT ticket_id, event_at, 'event' AS source
FROM ticket_events
UNION ALL
SELECT ticket_id, event_at, 'event' AS source
FROM ticket_events
WHERE ticket_id <= 14
