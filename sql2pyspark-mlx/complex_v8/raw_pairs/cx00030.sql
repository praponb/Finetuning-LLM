SELECT id AS key_id, priority AS name
FROM tickets
WHERE csat BETWEEN 2 AND 4
INTERSECT
SELECT id AS key_id, event_type AS name
FROM ticket_events
WHERE actor IS NOT NULL
ORDER BY name DESC
