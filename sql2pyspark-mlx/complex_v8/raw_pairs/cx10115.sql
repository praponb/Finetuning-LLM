SELECT id AS entity_id, agent_id AS entity_id_2, opened_at AS happened_at
FROM tickets
UNION
SELECT csat, requester_id, opened_at
FROM tickets
WHERE agent_id IS NOT NULL
ORDER BY entity_id DESC, happened_at
