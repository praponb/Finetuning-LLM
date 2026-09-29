SELECT id, actor
FROM ticket_events
WHERE ticket_id >= 27
  AND actor IN ('customer', 'agent')
EXCEPT
SELECT id, tier AS actor
FROM support_agents
WHERE tier <> 'T2'
