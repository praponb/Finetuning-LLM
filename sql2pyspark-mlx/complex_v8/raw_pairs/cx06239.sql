WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods
    FROM tracks
    WHERE bpm <= 166
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    moods,
    CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
    ARRAY_CONTAINS(moods, 'focus') AS has_focus
FROM filtered
WHERE is_explicit = FALSE
ORDER BY title_ref DESC, artist_id DESC NULLS FIRST, id
LIMIT 5
