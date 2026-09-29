SELECT birth_year, username AS category
FROM listeners
UNION
SELECT artist_id, title
FROM tracks
ORDER BY birth_year DESC, category
