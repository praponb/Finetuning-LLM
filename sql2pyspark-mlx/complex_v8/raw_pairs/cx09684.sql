SELECT id
FROM tickets
WHERE csat >= 1
  AND closed_at < TIMESTAMP '2025-03-05 00:00:00'
EXCEPT ALL
SELECT ticket_id
FROM ticket_events
WHERE event_type IN ('escalated', 'replied')
  AND ticket_id IS NOT NULL
ORDER BY id
