WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
      AND NOT ARRAY_CONTAINS(moods, 'workout')
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.moods,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM base s
    JOIN plays p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    title,
    release_date,
    is_explicit,
    moods,
    play_id,
    play_listener_id,
    play_played_at,
    play_device,
    ADD_MONTHS(release_date, 12) AS release_date_plus_12m,
    DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
FROM line_level_plays
ORDER BY play_played_at DESC, days_since_release_date DESC NULLS FIRST, id, play_id
LIMIT 10
