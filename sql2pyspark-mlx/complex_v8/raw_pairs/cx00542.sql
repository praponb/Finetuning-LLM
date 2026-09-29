SELECT device, played_at, 'play' AS src
FROM plays
UNION ALL
SELECT source AS device, played_at, 'play_2' AS src
FROM plays
UNION ALL
SELECT device, played_at, 'play_3' AS src
FROM plays
WHERE listener_id <= 4
ORDER BY src
