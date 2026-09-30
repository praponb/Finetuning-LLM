WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        COALESCE(ms_played, 0) AS ms_played_filled_2
    FROM plays
    WHERE source IS NOT NULL
      AND track_id IS NOT NULL
),
combined_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_filled,
        t.bpm AS track_bpm,
        t.release_date AS track_release_date
    FROM prepared_plays s
    LEFT JOIN tracks t ON t.id = s.track_id AND UPPER(TRIM(t.title)) LIKE 'R%'
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.source,
        s.track_release_date,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at
    FROM combined_details_track s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    track_release_date,
    play_id,
    play_played_at,
    ABS(ms_played - 183484) AS ms_played_distance
FROM with_children
ORDER BY play_id NULLS LAST
