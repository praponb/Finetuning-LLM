SELECT id
FROM tickets
WHERE priority = 'P2'
  AND channel <> 'chat'
EXCEPT
SELECT agent_id AS id
FROM tickets
ORDER BY id DESC
