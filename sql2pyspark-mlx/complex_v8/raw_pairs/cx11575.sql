SELECT duration_sec
FROM tracks
EXCEPT
SELECT ms_played
FROM plays
ORDER BY duration_sec DESC
