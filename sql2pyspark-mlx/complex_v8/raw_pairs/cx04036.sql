SELECT ticket_id AS key_id, event_at AS happened_at, 'event' AS src
FROM ticket_events
UNION ALL
SELECT requester_id AS key_id, opened_at AS happened_at, 'ticket' AS src
FROM tickets
UNION ALL
SELECT csat AS key_id, opened_at AS happened_at, 'ticket' AS src
FROM tickets
WHERE csat > 2
ORDER BY src
