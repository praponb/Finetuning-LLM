SELECT track_id, played_at, 'play' AS src
FROM plays
UNION ALL
SELECT id, played_at, 'play_2' AS src
FROM plays
UNION ALL
SELECT track_id, played_at, 'play_3' AS src
FROM plays
ORDER BY track_id DESC
