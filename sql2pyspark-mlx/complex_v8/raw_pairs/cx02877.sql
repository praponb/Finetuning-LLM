SELECT id AS entity_id
FROM tracks
WHERE bpm < 163
  AND duration_sec >= 183
INTERSECT
SELECT debut_year AS entity_id
FROM artists
WHERE artist_name IS NOT NULL
ORDER BY entity_id
