SELECT 'play' AS src, listener_id AS key_id, played_at AS happened_at
FROM plays
UNION ALL
SELECT 'play_2' AS src, ms_played, played_at
FROM plays
