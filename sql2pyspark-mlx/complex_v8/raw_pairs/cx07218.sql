SELECT requester_id, opened_at AS event_time
FROM tickets
WHERE MONTH(closed_at) IN (1, 3)
UNION ALL
SELECT id AS requester_id, event_at AS event_time
FROM ticket_events
WHERE event_type IS NOT NULL
