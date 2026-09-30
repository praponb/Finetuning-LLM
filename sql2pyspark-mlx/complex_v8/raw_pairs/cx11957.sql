SELECT agent_id AS qty, channel
FROM tickets
WHERE csat >= 3
EXCEPT ALL
SELECT requester_id AS qty, priority AS channel
FROM tickets
WHERE HOUR(opened_at) BETWEEN 9 AND 14
ORDER BY channel DESC
