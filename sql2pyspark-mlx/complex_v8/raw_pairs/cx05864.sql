WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    is_skipped,
    source,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM prepared
