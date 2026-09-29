WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
    WHERE listener_id <= 3
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM cleaned_plays s
    JOIN plays p ON p.track_id = s.track_id
),
combined_details_play AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.play_id,
        s.play_source,
        s.play_listener_id,
        s.play_ms_played,
        p.track_id AS play_track_id
    FROM exploded_detail s
    JOIN plays p ON p.id = s.play_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    play_id,
    play_source,
    play_listener_id,
    play_ms_played,
    play_track_id,
    CASE WHEN play_source IN ('search', 'radio') THEN 'group_a' WHEN play_source = 'album' THEN 'group_b' ELSE 'other' END AS play_source_group
FROM combined_details_play
WHERE play_listener_id <= 7
ORDER BY listener_id DESC NULLS FIRST, play_ms_played
