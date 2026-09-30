SELECT ticket_id
FROM ticket_events
WHERE ticket_id >= 57
  AND event_at >= TIMESTAMP '2025-01-13 14:00:00'
EXCEPT ALL
SELECT id AS ticket_id
FROM support_agents
ORDER BY ticket_id
