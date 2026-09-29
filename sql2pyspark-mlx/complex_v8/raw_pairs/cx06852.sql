SELECT birth_year AS ref_id
FROM listeners
WHERE ARRAY_CONTAINS(interests, 'podcasts')
  AND birth_year >= 1966
EXCEPT
SELECT bpm
FROM tracks
WHERE is_explicit = FALSE
