WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CONCAT_WS(': ', source, device) AS source_device_label
    FROM plays
),
enriched AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(source_device_label)) AS source_device_label_clean
    FROM scoped_plays
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.source_device_label,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device
    FROM enriched s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.source IN ('playlist', 'album', 'radio')
        )
      AND s.is_skipped = TRUE
),
deduped AS (
    -- Keep the latest row per track_id, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        source_device_label,
        play_id,
        play_is_skipped,
        play_device
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id DESC, play_id DESC) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    source_device_label,
    play_id,
    play_is_skipped,
    play_device
FROM deduped
ORDER BY listener_id, play_id NULLS LAST, id
LIMIT 10
