SELECT debut_year
FROM artists
WHERE UPPER(TRIM(artist_name)) LIKE 'N%'
INTERSECT
SELECT id
FROM plays
WHERE ms_played BETWEEN 26344 AND 302279
  AND listener_id IS NOT NULL
ORDER BY debut_year
