SELECT
    id,
    signup_date AS event_date,
    birth_year,
    'listener' AS src
FROM listeners
WHERE ARRAY_CONTAINS(interests, 'kpop')
  AND UPPER(TRIM(username)) LIKE 'F%'
UNION ALL
SELECT
    birth_year,
    signup_date,
    id,
    'listener_2' AS src
FROM listeners
UNION ALL
SELECT
    bpm,
    release_date,
    artist_id,
    'track' AS src
FROM tracks
WHERE UPPER(TRIM(title)) LIKE 'R%'
