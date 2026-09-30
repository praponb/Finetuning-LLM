SELECT priority, closed_at, 'ticket' AS record_type
FROM tickets
WHERE opened_at < TIMESTAMP '2025-03-02 08:00:00'
  AND priority = 'P2'
UNION ALL
SELECT channel AS priority, opened_at AS closed_at, 'ticket_2' AS record_type
FROM tickets
WHERE HOUR(closed_at) BETWEEN 9 AND 13
  AND agent_id <= 9
ORDER BY closed_at DESC
