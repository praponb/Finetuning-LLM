WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE track_id IS NOT NULL
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    source,
    CONCAT_WS(': ', source, device) AS source_device_label
FROM scoped_plays
ORDER BY device DESC NULLS FIRST, source_device_label NULLS LAST
