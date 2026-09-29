SELECT channel AS label, priority, opened_at AS happened_at
FROM tickets
WHERE priority = 'P1'
  AND requester_id <= 22
UNION ALL
SELECT priority AS label, channel AS priority, opened_at AS happened_at
FROM tickets
WHERE csat > 2
ORDER BY priority DESC, happened_at
