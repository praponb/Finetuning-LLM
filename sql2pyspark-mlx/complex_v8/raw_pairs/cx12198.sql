SELECT artist_name
FROM artists
EXCEPT
SELECT source AS artist_name
FROM plays
WHERE listener_id <= 9
  AND TO_DATE(played_at) < DATE '2025-02-12'
ORDER BY artist_name
