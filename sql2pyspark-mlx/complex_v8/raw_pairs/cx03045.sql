SELECT track_id, source AS name
FROM plays
INTERSECT
SELECT birth_year, username
FROM listeners
WHERE plan = 'free'
