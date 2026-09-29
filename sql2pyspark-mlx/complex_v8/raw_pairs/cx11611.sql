SELECT ticket_id AS key_id, event_at AS happened_at, 'event' AS origin
FROM ticket_events
UNION ALL
SELECT id, event_at, 'event_2' AS origin
FROM ticket_events
