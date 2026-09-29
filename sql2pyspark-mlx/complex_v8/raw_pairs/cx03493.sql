SELECT
    artist_id AS qty,
    bpm AS key_id,
    release_date,
    'track' AS origin
FROM tracks
WHERE is_explicit = TRUE
  AND release_date >= DATE '2024-08-26'
UNION ALL
SELECT
    bpm,
    artist_id,
    release_date,
    'track_2' AS origin
FROM tracks
WHERE ARRAY_CONTAINS(moods, 'party')
ORDER BY origin DESC
