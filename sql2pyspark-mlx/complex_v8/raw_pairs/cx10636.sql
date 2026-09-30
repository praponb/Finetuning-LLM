SELECT ms_played AS key_id
FROM plays
WHERE track_id IS NOT NULL
  AND HOUR(played_at) BETWEEN 6 AND 10
EXCEPT ALL
SELECT listener_id
FROM plays
ORDER BY key_id DESC
