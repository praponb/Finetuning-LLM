WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        bpm + duration_sec AS bpm_plus_duration_sec
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-06-03' AND DATE '2025-11-29'
      AND duration_sec >= 165
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM base_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.genre = 'Jazz'
        )
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.bpm_plus_duration_sec,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device
    FROM filtered s
    JOIN plays p ON p.track_id = s.id
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    play_id,
    play_played_at,
    play_is_skipped,
    play_device,
    DATE_TRUNC('MONTH', release_date) AS release_date_month,
    GREATEST(duration_sec, bpm_plus_duration_sec) AS greatest_duration_sec_bpm_plus_duration_sec
FROM line_level_plays
ORDER BY title DESC, id, play_id
LIMIT 20
