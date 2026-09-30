SELECT id, tier, 'agent' AS record_type
FROM support_agents
WHERE team <> 'VIP'
  AND tier IS NOT NULL
UNION ALL
SELECT ticket_id AS id, actor AS tier, 'event' AS record_type
FROM ticket_events
WHERE MONTH(event_at) IN (2, 3)
  AND actor IN ('system', 'agent')
