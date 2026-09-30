SELECT channel AS label, opened_at
FROM tickets
WHERE MONTH(closed_at) IN (2, 3)
  AND HOUR(opened_at) BETWEEN 7 AND 10
UNION ALL
SELECT actor, event_at
FROM ticket_events
WHERE event_type IN ('resolved', 'replied')
UNION ALL
SELECT event_type, event_at
FROM ticket_events
WHERE event_type NOT IN ('escalated', 'reopened')
ORDER BY label, opened_at
