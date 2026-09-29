SELECT id, genre AS name
FROM artists
WHERE debut_year >= 1991
EXCEPT
SELECT id, artist_name
FROM artists
WHERE is_verified = FALSE
ORDER BY name DESC
