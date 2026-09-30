SELECT id AS qty
FROM tickets
EXCEPT
SELECT agent_id
FROM tickets
WHERE agent_id IS NOT NULL
ORDER BY qty
