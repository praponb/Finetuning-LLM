WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS rn
        FROM prepared_tracks
    ) ranked
    WHERE rn = 1
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM latest s
)
-- Subtotals with CUBE
SELECT
    play_count,
    is_explicit,
    SUM(bpm) FILTER (WHERE NOT is_explicit) AS bpm_not_is_explicit
FROM with_stats
GROUP BY CUBE(play_count, is_explicit)
ORDER BY play_count NULLS LAST, is_explicit NULLS LAST
