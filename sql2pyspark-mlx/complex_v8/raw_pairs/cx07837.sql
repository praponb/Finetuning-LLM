SELECT id AS entity_id, event_at AS event_time
FROM ticket_events
UNION ALL
SELECT csat AS entity_id, closed_at AS event_time
FROM tickets
WHERE csat < 4
  AND ARRAY_CONTAINS(tags, 'login')
ORDER BY entity_id
