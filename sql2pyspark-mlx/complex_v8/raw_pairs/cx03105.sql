SELECT source AS category, device
FROM plays
WHERE played_at >= TIMESTAMP '2025-01-19 11:00:00'
  AND ms_played > 160264
UNION ALL
SELECT source, device
FROM plays
UNION ALL
SELECT country, username
FROM listeners
ORDER BY device, category
