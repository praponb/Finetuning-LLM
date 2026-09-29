SELECT birth_year
FROM listeners
INTERSECT
SELECT track_id AS birth_year
FROM plays
ORDER BY birth_year
