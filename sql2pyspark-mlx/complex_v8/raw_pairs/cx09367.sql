SELECT id AS qty, team AS label
FROM support_agents
WHERE tier IN ('T1', 'T2')
  AND team IN ('Tech', 'Onboarding')
EXCEPT
SELECT agent_id, channel
FROM tickets
