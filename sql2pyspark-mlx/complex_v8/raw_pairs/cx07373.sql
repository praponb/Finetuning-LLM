WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND NOT is_explicit
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_ref,
        t.mood
    FROM prepared s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM exploded s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm >= 82
              AND YEAR(t.release_date) = 2025
        )
      AND s.is_explicit = TRUE
)
SELECT
    is_explicit,
    artist_id,
    ROUND(100 * COUNT(CASE WHEN title IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_title_is_not_null
FROM with_activity
GROUP BY GROUPING SETS ((is_explicit, artist_id), (is_explicit), ())
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
