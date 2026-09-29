SELECT ticket_id AS ref_id, event_at AS happened_at, 'event' AS record_type
FROM ticket_events
WHERE ticket_id IS NOT NULL
UNION ALL
SELECT agent_id AS ref_id, closed_at AS happened_at, 'ticket' AS record_type
FROM tickets
WHERE csat >= 1
  AND priority IN ('P3', 'P2')
ORDER BY happened_at
