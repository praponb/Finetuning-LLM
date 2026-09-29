SELECT listener_id AS qty, device, played_at
FROM plays
WHERE ms_played <= 326250
  AND listener_id <= 8
UNION ALL
SELECT track_id AS qty, device, played_at
FROM plays
WHERE device IN ('tv', 'desktop')
ORDER BY device DESC, qty
