WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE (track_id IS NOT NULL OR source = 'search')
),
deduped AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at DESC, id DESC) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
)
SELECT
    listener_id,
    track_id,
    source,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE device = 'speaker') AS n_device_speaker,
    COUNT(*) FILTER (WHERE source IS NOT NULL) AS n_source_is_not_null,
    COUNT(*) FILTER (WHERE source = 'playlist') AS n_source_playlist
FROM deduped
GROUP BY GROUPING SETS ((listener_id, track_id, source), (listener_id), ())
