WITH cleaned AS (
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
    WHERE title IS NOT NULL
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM cleaned s
    LEFT JOIN plays p ON p.track_id = s.id
),
deduped AS (
    -- Keep the earliest row per artist_id, play_listener_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        play_id,
        play_listener_id,
        play_is_skipped,
        play_played_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id, play_listener_id ORDER BY duration_sec, id, play_id) AS recency_rank
        FROM detail_rows
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    is_explicit,
    moods,
    play_id,
    play_listener_id,
    play_is_skipped
FROM deduped
ORDER BY id DESC, play_listener_id DESC NULLS FIRST
