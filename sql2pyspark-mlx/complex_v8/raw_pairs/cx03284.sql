SELECT duration_sec AS key_id, title AS name
FROM tracks
WHERE duration_sec >= 165
  AND SIZE(moods) >= 1
EXCEPT ALL
SELECT artist_id AS key_id, title AS name
FROM tracks
WHERE bpm <= 142
