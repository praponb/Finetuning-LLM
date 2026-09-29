SELECT ms_played, played_at AS happened_at, device AS name
FROM plays
WHERE device IN ('tv', 'speaker', 'car')
  AND HOUR(played_at) BETWEEN 6 AND 9
UNION
SELECT track_id, played_at, source
FROM plays
