WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS(' / ', device, source) AS device_source_label,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE listener_id IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(ms_played_bucket, 0) AS ms_played_bucket_filled,
        ms_played + ms_played_bucket AS ms_played_plus_ms_played_bucket
    FROM filtered_plays
)
SELECT
    track_id,
    source,
    device,
    SUM(CASE WHEN source IN ('radio', 'album') THEN 1 ELSE 0 END) AS n_source_in_radio_album
FROM scored
GROUP BY CUBE(track_id, source, device)
ORDER BY track_id NULLS LAST, source NULLS LAST, device NULLS LAST
