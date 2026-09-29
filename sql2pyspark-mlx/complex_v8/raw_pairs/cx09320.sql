SELECT id AS ref_id, hired_on, team AS label
FROM support_agents
UNION ALL
SELECT id AS ref_id, hired_on, tier AS label
FROM support_agents
WHERE team NOT IN ('Billing', 'Onboarding')
