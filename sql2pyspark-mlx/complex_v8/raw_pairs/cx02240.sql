SELECT csat AS key_id, priority
FROM tickets
MINUS
SELECT requester_id AS key_id, priority
FROM tickets
