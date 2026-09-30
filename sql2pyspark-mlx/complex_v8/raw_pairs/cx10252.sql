WITH cleaned AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref
    FROM artists
    WHERE debut_year > 1996
      AND genre = 'HipHop'
),
with_children_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.genre,
        s.country,
        s.debut_year,
        s.is_verified,
        s.genre_ref,
        t.id AS track_id,
        t.title AS track_title,
        t.moods AS track_moods,
        t.release_date AS track_release_date,
        t.duration_sec AS track_duration_sec
    FROM cleaned s
    LEFT JOIN tracks t ON t.artist_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(country, 'ALL') AS country,
    track_id,
    is_verified,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt
FROM with_children_tracks
GROUP BY ROLLUP(country, track_id, is_verified)
ORDER BY country NULLS LAST, track_id NULLS LAST, is_verified NULLS LAST
