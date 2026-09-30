SELECT genre, debut_year
FROM artists
WHERE debut_year <= 2020
UNION
SELECT device, listener_id
FROM plays
WHERE listener_id >= 5
  AND track_id >= 29
ORDER BY genre DESC, debut_year
