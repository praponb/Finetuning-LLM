SELECT device, played_at AS event_time, id AS key_id
FROM plays
UNION ALL
SELECT source, played_at, listener_id
FROM plays
WHERE MONTH(played_at) IN (2, 3)
  AND source <> 'radio'
UNION ALL
SELECT source, played_at, track_id
FROM plays
WHERE ms_played BETWEEN 108979 AND 340114
