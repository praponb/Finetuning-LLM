SELECT
    title AS name,
    id,
    release_date,
    'track' AS origin
FROM tracks
UNION ALL
SELECT
    title,
    artist_id,
    release_date,
    'track_2' AS origin
FROM tracks
ORDER BY name
