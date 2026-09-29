SELECT ms_played AS entity_id
FROM plays
WHERE listener_id <= 13
  AND track_id IS NOT NULL
EXCEPT
SELECT id
FROM artists
WHERE debut_year < 2015
  AND is_verified = FALSE
ORDER BY entity_id DESC
