SELECT id AS key_id
FROM tickets
WHERE SIZE(tags) >= 2
EXCEPT
SELECT ticket_id
FROM ticket_events
ORDER BY key_id
