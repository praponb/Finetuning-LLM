SELECT ms_played, device
FROM plays
INTERSECT
SELECT debut_year AS ms_played, country AS device
FROM artists
ORDER BY ms_played DESC, device
