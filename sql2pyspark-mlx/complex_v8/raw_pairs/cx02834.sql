SELECT id
FROM artists
WHERE is_verified = FALSE
  AND UPPER(TRIM(artist_name)) LIKE 'S%'
INTERSECT
SELECT ms_played
FROM plays
WHERE ms_played <= 220498
  AND is_skipped = FALSE
