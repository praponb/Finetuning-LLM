SELECT agent_id AS ref_id, id, 'ticket' AS record_type
FROM tickets
UNION ALL
SELECT csat AS ref_id, agent_id AS id, 'ticket_2' AS record_type
FROM tickets
UNION ALL
SELECT requester_id AS ref_id, csat AS id, 'ticket_3' AS record_type
FROM tickets
WHERE channel IS NOT NULL
  AND priority = 'P1'
ORDER BY ref_id DESC, record_type
