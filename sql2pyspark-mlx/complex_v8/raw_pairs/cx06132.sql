SELECT id
FROM listeners
INTERSECT
SELECT bpm
FROM tracks
WHERE artist_id <= 7
  AND ARRAY_CONTAINS(moods, 'workout')
ORDER BY id DESC
