SELECT duration_sec, artist_id AS qty
FROM tracks
WHERE bpm >= 94
UNION ALL
SELECT id, birth_year
FROM listeners
