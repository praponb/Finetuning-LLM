WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
    WHERE is_explicit = TRUE
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.title_clean,
        t.mood_pos,
        t.mood
    FROM prepared_tracks s
    LATERAL VIEW OUTER POSEXPLODE(s.moods) t AS mood_pos, mood
)
-- Subtotals with CUBE
SELECT
    mood,
    artist_id,
    COUNT(*) AS n_rows,
    PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
FROM exploded
GROUP BY CUBE(mood, artist_id)
ORDER BY mood NULLS LAST, artist_id NULLS LAST
