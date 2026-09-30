SELECT id AS ref_id, team AS label
FROM support_agents
WHERE hired_on < DATE '2025-02-26'
UNION ALL
SELECT id, team
FROM support_agents
UNION ALL
SELECT agent_id, priority
FROM tickets
WHERE csat > 3
  AND opened_at < TIMESTAMP '2025-02-18 13:00:00'
