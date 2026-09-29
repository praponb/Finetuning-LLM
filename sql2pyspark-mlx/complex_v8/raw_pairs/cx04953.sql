SELECT csat, id AS key_id, priority
FROM tickets
UNION ALL
SELECT agent_id, id, priority
FROM tickets
ORDER BY csat DESC, priority
