SELECT csat AS qty, closed_at AS event_time, channel
FROM tickets
WHERE MONTH(closed_at) IN (1, 2)
UNION ALL
SELECT csat, opened_at, channel
FROM tickets
UNION ALL
SELECT ticket_id, event_at, actor
FROM ticket_events
