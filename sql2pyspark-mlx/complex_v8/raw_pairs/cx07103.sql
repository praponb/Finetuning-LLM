SELECT tier
FROM support_agents
WHERE team = 'Onboarding'
  AND tier = 'T3'
EXCEPT ALL
SELECT priority
FROM tickets
ORDER BY tier DESC
