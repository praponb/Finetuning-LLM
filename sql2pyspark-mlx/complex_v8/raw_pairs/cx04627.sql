SELECT priority AS category, opened_at AS happened_at, channel AS category_2
FROM tickets
WHERE requester_id >= 3
  AND TO_DATE(opened_at) < DATE '2025-03-02'
UNION ALL
SELECT priority, closed_at, channel
FROM tickets
WHERE SIZE(tags) >= 2
  AND priority IN ('P1', 'P2')
UNION ALL
SELECT actor, event_at, event_type
FROM ticket_events
ORDER BY happened_at DESC
