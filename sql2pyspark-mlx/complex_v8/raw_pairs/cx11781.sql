WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-19 14:00:00'
      AND is_skipped = FALSE
),
deduped AS (
    -- Keep the latest row per track_id, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, source ORDER BY ms_played DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        MAX(ms_played) AS max_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.max_play_ms_played
    FROM deduped s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source,
    last_played_at,
    max_play_ms_played,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label,
    CONCAT_WS('-', device, source) AS device_source_label
FROM with_counts
ORDER BY listener_id NULLS LAST, last_played_at NULLS LAST, id
LIMIT 20
