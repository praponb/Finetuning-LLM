SELECT id AS key_id
FROM support_agents
INTERSECT
SELECT ticket_id AS key_id
FROM ticket_events
WHERE MONTH(event_at) IN (1, 2)
  AND event_type IS NOT NULL
