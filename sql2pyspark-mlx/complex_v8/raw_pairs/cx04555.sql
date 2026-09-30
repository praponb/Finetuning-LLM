WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE NOT is_explicit
      AND SIZE(moods) >= 2
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_prefix,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM filtered_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.country NOT IN ('US', 'JP')
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM joined_artist s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.listener_id >= 13
              AND p.is_skipped = FALSE
        )
)
-- Subtotals with GROUPING SETS
SELECT
    is_explicit,
    artist_is_verified,
    artist_id,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM without_matches
GROUP BY GROUPING SETS ((is_explicit, artist_is_verified, artist_id), (is_explicit), ())
ORDER BY is_explicit NULLS LAST, artist_is_verified NULLS LAST, artist_id NULLS LAST
