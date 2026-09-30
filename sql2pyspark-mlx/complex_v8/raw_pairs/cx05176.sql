WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        ABS(ms_played - 151112) AS ms_played_distance
    FROM plays
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_distance,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM prepared_plays s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT
    id,
    track_id,
    source,
    ms_played_distance,
    play_id,
    play_played_at
FROM detail_rows
WHERE is_skipped = TRUE
ORDER BY track_id DESC, source DESC NULLS FIRST
