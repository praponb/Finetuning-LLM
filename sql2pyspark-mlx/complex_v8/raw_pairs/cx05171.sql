SELECT birth_year AS ref_id, username
FROM listeners
INTERSECT ALL
SELECT track_id AS ref_id, device AS username
FROM plays
ORDER BY username, ref_id
