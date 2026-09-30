WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CONCAT_WS(' / ', source, device) AS source_device_label
    FROM plays
    WHERE source = 'search'
)
SELECT id, played_at
FROM filtered_plays
UNION ALL
SELECT id, played_at
FROM plays
ORDER BY id
