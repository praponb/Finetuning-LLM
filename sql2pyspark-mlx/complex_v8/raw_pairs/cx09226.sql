SELECT debut_year, id AS key_id
FROM artists
WHERE is_verified = TRUE
UNION ALL
SELECT id AS debut_year, birth_year AS key_id
FROM listeners
WHERE plan IS NOT NULL
  AND birth_year >= 1969
UNION ALL
SELECT id AS debut_year, ms_played AS key_id
FROM plays
ORDER BY debut_year DESC
