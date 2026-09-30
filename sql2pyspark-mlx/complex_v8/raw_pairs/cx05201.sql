SELECT priority AS label, closed_at
FROM tickets
WHERE agent_id >= 9
UNION ALL
SELECT channel, closed_at
FROM tickets
WHERE csat > 3
ORDER BY closed_at
