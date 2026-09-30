SELECT csat, priority
FROM tickets
INTERSECT
SELECT id, channel
FROM tickets
WHERE agent_id >= 6
  AND priority = 'P2'
