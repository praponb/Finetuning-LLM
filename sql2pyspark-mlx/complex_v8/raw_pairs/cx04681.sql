WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-10-23' AND DATE '2025-11-29'
      AND is_explicit = TRUE
)
SELECT
    id,
    title,
    is_explicit,
    moods,
    SUBSTRING(TRIM(title), 1, 4) AS title_prefix
FROM prepared
ORDER BY title NULLS LAST
