SELECT id, priority AS label, closed_at AS event_time
FROM tickets
WHERE HOUR(opened_at) BETWEEN 7 AND 12
UNION ALL
SELECT id, actor, event_at
FROM ticket_events
WHERE actor NOT IN ('customer', 'system')
UNION ALL
SELECT csat, priority, closed_at
FROM tickets
WHERE agent_id >= 5
