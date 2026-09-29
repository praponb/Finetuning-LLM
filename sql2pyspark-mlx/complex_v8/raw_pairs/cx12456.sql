SELECT 'listener' AS src, plan AS label, id AS ref_id
FROM listeners
UNION ALL
SELECT 'artist' AS src, country AS label, id AS ref_id
FROM artists
UNION ALL
SELECT 'track' AS src, title AS label, id AS ref_id
FROM tracks
WHERE ARRAY_CONTAINS(moods, 'workout')
  AND LOWER(title) LIKE '%dr%'
