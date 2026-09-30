WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 4
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.country AS artist_country,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM without_matches s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    artist_country,
    is_explicit,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(artist_country)) AS artist_country_list
FROM with_parent_artist
GROUP BY ROLLUP(artist_id, artist_country, is_explicit)
ORDER BY artist_id NULLS LAST, artist_country NULLS LAST, is_explicit NULLS LAST
