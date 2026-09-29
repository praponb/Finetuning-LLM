SELECT birth_year AS qty
FROM listeners
WHERE plan IN ('premium', 'student')
  AND ARRAY_CONTAINS(interests, 'live')
INTERSECT
SELECT duration_sec
FROM tracks
WHERE bpm > 89
ORDER BY qty
