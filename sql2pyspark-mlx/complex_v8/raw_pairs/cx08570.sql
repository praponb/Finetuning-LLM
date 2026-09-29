SELECT id, track_id, played_at AS happened_at
FROM plays
UNION ALL
SELECT listener_id AS id, track_id, played_at AS happened_at
FROM plays
ORDER BY track_id
