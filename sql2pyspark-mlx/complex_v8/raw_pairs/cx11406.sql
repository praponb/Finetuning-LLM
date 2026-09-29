WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods
    FROM tracks
    WHERE duration_sec > 144
      AND ARRAY_CONTAINS(moods, 'chill')
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        t.mood
    FROM filtered_tracks s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Subtotals with GROUPING SETS
SELECT
    artist_id,
    is_explicit,
    GROUPING(artist_id) AS is_total_artist_id,
    ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
    COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false
FROM flattened
GROUP BY GROUPING SETS ((artist_id, is_explicit), (artist_id))
ORDER BY artist_id NULLS LAST, is_explicit NULLS LAST
