SELECT track_id
FROM plays
EXCEPT ALL
SELECT bpm
FROM tracks
ORDER BY track_id DESC
