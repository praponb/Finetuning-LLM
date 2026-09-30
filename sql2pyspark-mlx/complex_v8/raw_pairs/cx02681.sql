SELECT birth_year AS qty, username, 'listener' AS src
FROM listeners
WHERE signup_date IS NOT NULL
UNION
SELECT duration_sec, title, 'track' AS src
FROM tracks
WHERE artist_id >= 3
  AND ARRAY_CONTAINS(moods, 'workout')
ORDER BY qty, src
