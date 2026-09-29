SELECT ticket_id AS key_id
FROM ticket_events
WHERE actor IN ('system', 'customer')
  AND ticket_id IS NOT NULL
INTERSECT
SELECT id AS key_id
FROM ticket_events
WHERE event_type NOT IN ('assigned', 'resolved')
  AND actor IN ('system', 'customer')
ORDER BY key_id DESC
