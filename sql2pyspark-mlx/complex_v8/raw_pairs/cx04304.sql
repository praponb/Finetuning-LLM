WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'focus')
)
SELECT title_prefix
FROM filtered
INTERSECT
SELECT country
FROM listeners
ORDER BY title_prefix DESC
