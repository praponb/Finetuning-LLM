WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE title IS NOT NULL
      AND ARRAY_CONTAINS(moods, 'workout')
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        bpm,
        artist_genre,
        bpm - LAG(bpm) OVER (ORDER BY bpm ASC, id) AS bpm_change,
        LAG(bpm, 1, 0) OVER w_all_by_bpm AS prev_bpm
    FROM enriched_artist
    WINDOW w_all_by_bpm AS (ORDER BY bpm, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(bpm / NULLIF(bpm_change, 0), 3) AS bpm_per_bpm_change,
        COALESCE(prev_bpm, 0) AS prev_bpm_filled,
        LOWER(REGEXP_REPLACE(TRIM(artist_genre), ' ', '_')) AS artist_genre_slug
    FROM with_windows
    WHERE bpm_change <= 90
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.artist_genre,
        s.bpm_change,
        s.prev_bpm_filled,
        s.artist_genre_slug,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM annotated s
    JOIN plays p ON p.track_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT artist_id, play_played_at, artist_genre, prev_bpm_filled
    FROM line_level
) src
PIVOT (
    COUNT(prev_bpm_filled)
    FOR artist_genre IN ('Classical', 'Pop', 'HipHop', 'Jazz')
)
ORDER BY artist_id, play_played_at
