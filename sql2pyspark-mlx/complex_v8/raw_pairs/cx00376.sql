SELECT actor AS label, ticket_id AS key_id, 'event' AS origin
FROM ticket_events
UNION ALL
SELECT channel, csat, 'ticket' AS origin
FROM tickets
WHERE SIZE(tags) >= 2
  AND requester_id >= 4
