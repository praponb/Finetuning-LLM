WITH cleaned AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified
    FROM artists
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
        )
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.is_explicit = FALSE) FROM tracks t WHERE t.artist_id = s.id) AS track_is_explicit_count,
        (SELECT MIN(t.duration_sec) FROM tracks t WHERE t.artist_id = s.id) AS min_track_duration_sec,
        (SELECT SUM(t.duration_sec) FROM tracks t WHERE t.artist_id = s.id) AS total_track_duration_sec
    FROM qualified s
)
-- Subtotals with ROLLUP
SELECT
    genre,
    track_is_explicit_count,
    country,
    ARRAY_SORT(COLLECT_SET(country)) AS country_list
FROM with_counts
GROUP BY ROLLUP(genre, track_is_explicit_count, country)
ORDER BY genre NULLS LAST, track_is_explicit_count NULLS LAST, country NULLS LAST
